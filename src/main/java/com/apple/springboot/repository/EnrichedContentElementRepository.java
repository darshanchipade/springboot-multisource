package com.apple.springboot.repository;

import com.apple.springboot.model.EnrichedContentElement;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface EnrichedContentElementRepository extends JpaRepository<EnrichedContentElement, UUID> {
    // Add custom query methods here if needed
    List<EnrichedContentElement> findAllByCleansedDataId(UUID cleansedDataId);
    List<EnrichedContentElement> findAllByCleansedDataIdAndVersion(UUID cleansedDataId, Integer version);
    //Optional<EnrichedContentElement> findByItemSourcePathAndContentHash(String itemSourcePath, String contentHash);
    Optional<EnrichedContentElement> findByItemSourcePathAndItemOriginalFieldName(String itemSourcePath, String itemOriginalFieldName);
}