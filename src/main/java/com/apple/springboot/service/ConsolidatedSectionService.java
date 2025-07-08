package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.ConsolidatedEnrichedSection;
import com.apple.springboot.model.EnrichedContentElement;
import com.apple.springboot.repository.ConsolidatedEnrichedSectionRepository;
import com.apple.springboot.repository.EnrichedContentElementRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Service responsible for consolidating enriched content elements
 * into aggregated section-level views stored in  ConsolidatedEnrichedSection.
 * <p>
 * This service was built for the search interface which we will be building in future
 * 
 */
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
        System.out.println("Enriched items found: " + enrichedItems.size());

        enrichedItems.forEach(item -> {
            if (item.getItemSourcePath() == null || item.getCleansedText() == null) return;

            boolean exists = consolidatedRepo.existsBySectionUriAndSectionPathAndCleansedText(
                    item.getSourceUri(), item.getItemSourcePath(), item.getCleansedText());

            if (!exists) {
                ConsolidatedEnrichedSection section = new ConsolidatedEnrichedSection();
                section.setId(UUID.randomUUID());
                section.setCleansedDataId(cleansedData.getId());
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
                section.setSavedAt(OffsetDateTime.now());
                section.setStatus(item.getStatus());

                consolidatedRepo.save(section);
            }
        });
    }
}