package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.ConsolidatedEnrichedSection;
import com.apple.springboot.model.EnrichedContentElement;
import com.apple.springboot.repository.ConsolidatedEnrichedSectionRepository;
import com.apple.springboot.repository.EnrichedContentElementRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import jakarta.transaction.Transactional;

import java.time.OffsetDateTime;
import java.util.List;

@Service
public class ConsolidatedSectionService {

    private static final Logger logger = LoggerFactory.getLogger(ConsolidatedSectionService.class);

    private final EnrichedContentElementRepository enrichedRepo;
    private final ConsolidatedEnrichedSectionRepository consolidatedRepo;

    public ConsolidatedSectionService(EnrichedContentElementRepository enrichedRepo,
                                      ConsolidatedEnrichedSectionRepository consolidatedRepo) {
        this.enrichedRepo = enrichedRepo;
        this.consolidatedRepo = consolidatedRepo;
    }

    @Transactional
    public void saveFromCleansedEntry(CleansedDataStore cleansedData) {
        List<EnrichedContentElement> enrichedItems = enrichedRepo.findAllByCleansedDataId(cleansedData.getId());
        logger.info("Found {} enriched items for CleansedDataStore ID: {} to consolidate.", enrichedItems.size(), cleansedData.getId());

        enrichedItems.forEach(item -> {
            if (item.getItemSourcePath() == null || item.getCleansedText() == null) {
                logger.warn("Skipping enriched item ID {} due to null itemSourcePath or cleansedText.", item.getId());
                return;
            }

            boolean exists = consolidatedRepo.existsBySectionUriAndSectionPathAndCleansedTextAndVersion(
                    item.getSourceUri(), item.getItemSourcePath(), item.getCleansedText(), cleansedData.getVersion());

            if (!exists) {
                ConsolidatedEnrichedSection section = new ConsolidatedEnrichedSection();
                section.setCleansedDataId(cleansedData.getId());
                section.setVersion(cleansedData.getVersion());
                section.setSourceUri(item.getSourceUri());
                section.setSectionPath(item.getItemSourcePath());
                section.setOriginalFieldName(item.getItemOriginalFieldName());
                section.setCleansedText(item.getCleansedText());
                section.setSummary(item.getSummary());
                section.setClassification(item.getClassification());
                section.setKeywords(item.getKeywords());
                section.setTags(item.getTags());
                section.setSentiment(item.getSentiment());
                section.setModelUsed(item.getBedrockModelUsed());
                section.setEnrichmentMetadata(item.getEnrichmentMetadata());
                section.setEnrichedAt(item.getEnrichedAt());
                section.setSectionUri(item.getItemSourcePath());
                section.setContext(item.getContext());
                section.setSavedAt(OffsetDateTime.now());
                section.setStatus(item.getStatus());

                consolidatedRepo.save(section);
                logger.info("Saved new ConsolidatedEnrichedSection ID {} from EnrichedContentElement ID {}", section.getId(), item.getId());
            } else {
                logger.info("ConsolidatedEnrichedSection already exists for itemSourcePath '{}' and cleansedText snippet. Skipping save.", item.getItemSourcePath());
            }
        });
    }
}