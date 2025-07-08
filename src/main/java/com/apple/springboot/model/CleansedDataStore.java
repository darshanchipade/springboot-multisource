package com.apple.springboot.model;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Getter;
import org.hibernate.annotations.*;
import software.amazon.awssdk.services.s3.endpoints.internal.Value;

import javax.persistence.*;
import javax.persistence.Entity;
import javax.persistence.Table;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;

@Getter
@Entity
@Table(name = "cleansed_data_store")
@TypeDefs({
        @TypeDef(name = "jsonb", typeClass = JsonBinaryType.class)  // Register jsonb mapping
})

public class CleansedDataStore {

    // Getters and Setters
    @Id
    @GeneratedValue(generator = "UUID")
    @GenericGenerator(
            name = "UUID",
            strategy = "org.hibernate.id.UUIDGenerator"
    )
    @Column(updatable = false, nullable = false)
    private UUID id;

    @Column(name = "raw_data_id", nullable = false)
    private UUID rawDataId;

    @Column(name = "source_uri", nullable = false, columnDefinition = "TEXT")
    private String sourceUri;

    @Type(type = "jsonb")
    //@Type(type = "com.vladmihalcea.hibernate.type.json.JsonStringType")
    @Column(name = "cleansed_items", nullable = false, columnDefinition = "jsonb")
    //private String cleansedItems;
    private List<Map<String, Object>> cleansedItems;

    @Type(type = "jsonb")
    //@Type(type = "com.vladmihalcea.hibernate.type.json.JsonStringType")
    @Column(name = "cleansing_errors", columnDefinition = "jsonb")
    private Map<String, Object> cleansingErrors;

    @CreationTimestamp
    @Column(name = "cleansed_at", nullable = false, updatable = false)
    private OffsetDateTime cleansedAt;

    @Column(name = "status", nullable = false, columnDefinition = "TEXT")
    private String status;

    public CleansedDataStore() {}

    public void setId(UUID id) { this.id = id; }

    public void setRawDataId(UUID rawDataId) { this.rawDataId = rawDataId; }

    public void setSourceUri(String sourceUri) { this.sourceUri = sourceUri; }

    public void setCleansedItems(List<Map<String,Object>>cleansedItems) { this.cleansedItems = cleansedItems; }

    public void setCleansingErrors(Map<String,Object> cleansingErrors) { this.cleansingErrors = cleansingErrors; }

    public void setCleansedAt(OffsetDateTime cleansedAt) { this.cleansedAt = cleansedAt; }

    public void setStatus(String status) { this.status = status; }
}