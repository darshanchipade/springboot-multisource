package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.ConsolidatedEnrichedSection;
import com.apple.springboot.model.EnrichedContentElement;
import com.apple.springboot.repository.ConsolidatedEnrichedSectionRepository;
import com.apple.springboot.repository.ContentHashRepository;
import com.apple.springboot.repository.EnrichedContentElementRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Service
public class ConsolidatedSectionService {

    private static final Logger logger = LoggerFactory.getLogger(ConsolidatedSectionService.class);

    private final EnrichedContentElementRepository enrichedRepo;
    private final ConsolidatedEnrichedSectionRepository consolidatedRepo;
    private final ContentHashRepository contentHashRepository;
    private static final String USAGE_REF_DELIM = " ::ref:: ";

    public ConsolidatedSectionService(EnrichedContentElementRepository enrichedRepo,
                                      ConsolidatedEnrichedSectionRepository consolidatedRepo,
                                      ContentHashRepository contentHashRepository) {
        this.enrichedRepo = enrichedRepo;
        this.consolidatedRepo = consolidatedRepo;
        this.contentHashRepository = contentHashRepository;
    }

    @Transactional
    public void saveFromCleansedEntry(CleansedDataStore cleansedData) {
        List<EnrichedContentElement> enrichedItems = enrichedRepo.findAllByCleansedDataId(cleansedData.getId());
        logger.info("Found {} enriched items for CleansedDataStore ID: {} to consolidate.", enrichedItems.size(), cleansedData.getId());

        for (EnrichedContentElement item : enrichedItems) {
            if (item.getItemSourcePath() == null || item.getCleansedText() == null) {
                logger.warn("Skipping enriched item ID {} due to null itemSourcePath or cleansedText.", item.getId());
                continue;
            }
            String usagePath = extractUsagePath(item); // may be "container ::ref:: fragment" or just a single path
            String[] split = splitUsagePath(usagePath);
            String sectionPath = split[0];   // container/placement
            String sectionUri  = split[1];   // fragment/canonical (where the copy lives)

            if (sectionPath == null) sectionPath = item.getItemSourcePath();
            if (sectionUri  == null) sectionUri  = item.getItemSourcePath();

            // This simple check prevents re-inserting the exact same item if the process is re-run.
            boolean exists = consolidatedRepo.existsBySectionUriAndSectionPathAndCleansedTextAndVersion(
                    item.getSourceUri(), item.getItemSourcePath(), item.getCleansedText(), cleansedData.getVersion());

            if (!exists) {
                ConsolidatedEnrichedSection section = new ConsolidatedEnrichedSection();
                section.setCleansedDataId(cleansedData.getId());
                section.setVersion(cleansedData.getVersion());
                section.setSourceUri(item.getSourceUri());           // file/source that produced this
                section.setSectionPath(sectionPath);                 // container
                section.setSectionUri(sectionUri);                  // fragment
                section.setOriginalFieldName(item.getItemOriginalFieldName());
                section.setCleansedText(item.getCleansedText());
                contentHashRepository.findBySourcePathAndItemType(item.getItemSourcePath(), item.getItemOriginalFieldName())
                        .ifPresent(contentHash -> section.setContentHash(contentHash.getContentHash()));
                // Note: contentHash would need to be copied from the 'item' if it existed on that entity.
                // section.setContentHash(item.getContentHash());
                section.setSummary(item.getSummary());
                section.setClassification(item.getClassification());
                section.setKeywords(item.getKeywords());
                section.setTags(item.getTags());
                section.setSentiment(item.getSentiment());
                section.setModelUsed(item.getBedrockModelUsed());
                section.setEnrichmentMetadata(item.getEnrichmentMetadata());
                section.setEnrichedAt(item.getEnrichedAt());
                section.setContext(item.getContext());
                section.setSavedAt(OffsetDateTime.now());
                section.setStatus(item.getStatus());

                consolidatedRepo.save(section);
                logger.info("Saved new ConsolidatedEnrichedSection ID {} from EnrichedContentElement ID {}", section.getId(), item.getId());
            } else {
                logger.info("ConsolidatedEnrichedSection already exists for itemSourcePath '{}' and cleansedText snippet. Skipping save.", item.getItemSourcePath());
            }
        }
    }

    @Transactional(readOnly = true)
    public List<ConsolidatedEnrichedSection> getSectionsFor(CleansedDataStore cleansedData) {
        if (cleansedData == null || cleansedData.getId() == null) {
            return Collections.emptyList();
        }
        if (cleansedData.getVersion() == null) {
            return consolidatedRepo.findAllByCleansedDataId(cleansedData.getId());
        }
        return consolidatedRepo.findAllByCleansedDataIdAndVersion(
                cleansedData.getId(), cleansedData.getVersion());
    }

    @SuppressWarnings("unchecked")
    private String extractUsagePath(EnrichedContentElement item) {
        Map<String, Object> ctx = item.getContext();
        if (ctx != null) {
            Object envObj = ctx.get("envelope");
            if (envObj instanceof Map<?, ?> env) {
                Object up = env.get("usagePath");
                if (up instanceof String s && !s.isBlank()) {
                    return s;
                }
            }
        }
        return item.getItemSourcePath();
    }

    private String[] splitUsagePath(String usagePath) {
        if (usagePath == null || usagePath.isBlank()) return new String[]{null, null};
        int idx = usagePath.indexOf(USAGE_REF_DELIM);
        if (idx < 0) return new String[]{usagePath, usagePath};
        String left = usagePath.substring(0, idx).trim();
        String right = usagePath.substring(idx + USAGE_REF_DELIM.length()).trim();
        return new String[]{left.isEmpty() ? null : left, right.isEmpty() ? null : right};
    }
}