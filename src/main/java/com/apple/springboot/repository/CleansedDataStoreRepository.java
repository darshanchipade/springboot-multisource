package com.apple.springboot.repository;

import com.apple.springboot.model.CleansedDataStore;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.UUID;

@Repository
public interface CleansedDataStoreRepository extends JpaRepository<CleansedDataStore, UUID> {
}