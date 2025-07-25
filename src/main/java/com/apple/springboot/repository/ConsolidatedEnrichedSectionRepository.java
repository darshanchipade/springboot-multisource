package com.apple.springboot.repository;

import com.apple.springboot.model.ConsolidatedEnrichedSection;
import com.apple.springboot.model.EnrichedContentElement;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface ConsolidatedEnrichedSectionRepository extends JpaRepository<ConsolidatedEnrichedSection, UUID> {
    boolean existsBySectionUriAndSectionPathAndCleansedTextAndVersion(String sectionUri, String sectionPath, String cleansedText, Integer version);
    //boolean existsBySectionUriAndSectionPathAndCleansedText(String sectionUri, String sectionPath, String cleansedText);
    Optional<ConsolidatedEnrichedSection> findBySectionPathAndOriginalFieldName(String sectionPath, String originalFieldName);
}
