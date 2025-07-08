package com.apple.springboot.repository;

import com.apple.springboot.model.RawDataStore;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface RawDataStoreRepository extends JpaRepository<RawDataStore, UUID> {
    Optional<RawDataStore> findBySourceUri(String sourceUri);
}