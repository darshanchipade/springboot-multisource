package com.apple.springboot.repository;

import com.apple.springboot.model.ContentHash;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ContentHashRepository extends JpaRepository<ContentHash, ContentHash.ContentHashId> {
    Optional<ContentHash> findBySourcePathAndItemType(String sourcePath, String itemType);
}