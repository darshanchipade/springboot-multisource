package com.apple.springboot.repository;

import com.apple.springboot.model.ContentHash;
import com.apple.springboot.model.ContentHashId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ContentHashRepository extends JpaRepository<ContentHash, ContentHashId> {
    Optional<ContentHash> findBySourcePathAndItemType(String sourcePath, String itemType);
  //  boolean existsBySourcePathAndItemTypeAndContentHash(String sourcePath, String itemType, String contentHash);
}