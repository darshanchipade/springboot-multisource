package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.ConsolidatedEnrichedSection;
import com.apple.springboot.model.ContentChunk;
import com.apple.springboot.model.JobTracker;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.ContentChunkRepository;
import com.apple.springboot.repository.EnrichedContentElementRepository;
import com.apple.springboot.repository.JobTrackerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class JobCompletionService {

    private static final Logger logger = LoggerFactory.getLogger(JobCompletionService.class);

    private final JobTrackerRepository jobTrackerRepository;
    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final ConsolidatedSectionService consolidatedSectionService;
    private final TextChunkingService textChunkingService;
    private final BedrockEnrichmentService bedrockEnrichmentService;
    private final ContentChunkRepository contentChunkRepository;
    private final EnrichedContentElementRepository enrichedContentElementRepository;

    public JobCompletionService(JobTrackerRepository jobTrackerRepository,
                                CleansedDataStoreRepository cleansedDataStoreRepository,
                                ConsolidatedSectionService consolidatedSectionService,
                                TextChunkingService textChunkingService,
                                BedrockEnrichmentService bedrockEnrichmentService,
                                ContentChunkRepository contentChunkRepository,
                                EnrichedContentElementRepository enrichedContentElementRepository) {
        this.jobTrackerRepository = jobTrackerRepository;
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.consolidatedSectionService = consolidatedSectionService;
        this.textChunkingService = textChunkingService;
        this.bedrockEnrichmentService = bedrockEnrichmentService;
        this.contentChunkRepository = contentChunkRepository;
        this.enrichedContentElementRepository = enrichedContentElementRepository;
    }

    @Transactional
    public void finalizeJob(String jobId) {
        JobTracker jobTracker = jobTrackerRepository.findById(jobId)
                .orElseThrow(() -> new IllegalStateException("JobTracker not found for ID: " + jobId));

        CleansedDataStore cleansedDataEntry = cleansedDataStoreRepository.findById(jobTracker.getCleansedDataStoreId())
                .orElseThrow(() -> new IllegalStateException("CleansedDataStore not found for ID: " + jobTracker.getCleansedDataStoreId()));

        logger.info("Finalizing job {} for CleansedDataStore ID: {}", jobId, cleansedDataEntry.getId());

        // 1. Consolidate enriched data
        consolidatedSectionService.saveFromCleansedEntry(cleansedDataEntry);
        logger.info("Consolidated enriched sections for job {}", jobId);

        // 2. Create content chunks and embeddings
        List<ConsolidatedEnrichedSection> savedSections = consolidatedSectionService.getSectionsFor(cleansedDataEntry);
        List<String> errors = createContentChunksInBatch(savedSections);
        logger.info("Created content chunks for job {}. Errors during chunking: {}", jobId, errors.size());

        // 3. Update final status
        updateFinalCleansedDataStatus(cleansedDataEntry, jobTracker, errors);

        jobTracker.setStatus("COMPLETED");
        jobTracker.setUpdatedAt(OffsetDateTime.now());
        jobTrackerRepository.save(jobTracker);

        logger.info("Successfully finalized job {}", jobId);
    }

    private List<String> createContentChunksInBatch(List<ConsolidatedEnrichedSection> sections) {
        List<String> itemProcessingErrors = new ArrayList<>();
        if (sections == null || sections.isEmpty()) return itemProcessingErrors;

        List<String> allChunkTexts = new ArrayList<>();
        List<ContentChunk> placeholders = new ArrayList<>();

        for (ConsolidatedEnrichedSection section : sections) {
            String text = section.getCleansedText();
            if (text == null || text.isBlank()) {
                logger.debug("Section {} has empty cleansedText; skipping chunking.", section.getSectionPath());
                continue;
            }
            List<String> chunks = textChunkingService.chunkIfNeeded(text);
            if (chunks == null || chunks.isEmpty()) continue;

            for (String chunkText : chunks) {
                allChunkTexts.add(chunkText);
                ContentChunk p = new ContentChunk();
                p.setConsolidatedEnrichedSection(section);
                p.setChunkText(chunkText);
                p.setSourceField(section.getSourceUri());
                p.setSectionPath(section.getSectionPath());
                p.setCreatedAt(OffsetDateTime.now());
                p.setCreatedBy("EnrichmentPipelineService");
                placeholders.add(p);
            }
        }

        if (allChunkTexts.isEmpty()) {
            logger.info("No chunk texts to embed; skipping chunk creation.");
            return itemProcessingErrors;
        }

        try {
            List<float[]> vectors = bedrockEnrichmentService.generateEmbeddingsInBatch(allChunkTexts);

            int saved = 0;
            int n = Math.min(placeholders.size(), vectors.size());
            for (int i = 0; i < n; i++) {
                try {
                    ContentChunk chunk = placeholders.get(i);
                    chunk.setVector(vectors.get(i));
                    contentChunkRepository.save(chunk);
                    saved++;
                } catch (Exception e) {
                    logger.warn("Failed to save chunk index {} path {}: {}", i, placeholders.get(i).getSectionPath(), e.toString());
                }
            }

            if (vectors.size() != placeholders.size()) {
                String warn = String.format(
                        "Chunk/embedding size mismatch: chunks=%d, embeddings=%d. Saved first %d.",
                        placeholders.size(), vectors.size(), saved
                );
                logger.warn(warn);
                itemProcessingErrors.add(warn);
            }

            logger.info("Saved {} content chunks (requested {}).", saved, placeholders.size());
        } catch (Exception e) {
            String msg = "Embedding step failed; no chunks saved. " + e.getClass().getSimpleName() + ": " + e.getMessage();
            logger.error(msg, e);
            itemProcessingErrors.add(msg);
        }
        return itemProcessingErrors;
    }

    private void updateFinalCleansedDataStatus(CleansedDataStore cleansedDataEntry, JobTracker jobTracker, List<String> errors) {
        long successCount = enrichedContentElementRepository.countByCleansedDataIdAndStatus(cleansedDataEntry.getId(), "ENRICHED");
        long failureCount = jobTracker.getFailureCount();

        String finalStatus;
        if (failureCount == 0 && successCount == jobTracker.getTotalItems()) {
            finalStatus = "ENRICHED_COMPLETE";
        } else if (successCount > 0) {
            finalStatus = "PARTIALLY_ENRICHED";
        } else {
            finalStatus = "ENRICHMENT_FAILED";
        }
        cleansedDataEntry.setStatus(finalStatus);

        Map<String, Object> summary = new HashMap<>();
        summary.put("totalItems", jobTracker.getTotalItems());
        summary.put("successfullyEnriched", successCount);
        summary.put("failedEnrichment", failureCount);
        summary.put("errors", errors);
        try {
            cleansedDataEntry.setCleansingErrors(summary);
        } catch (Exception e) {
            logger.error("Could not set processing summary", e);
        }

        cleansedDataStoreRepository.save(cleansedDataEntry);
        logger.info("Finished enrichment for CleansedDataStore ID: {}. Final status: {}", cleansedDataEntry.getId(), finalStatus);
    }
}