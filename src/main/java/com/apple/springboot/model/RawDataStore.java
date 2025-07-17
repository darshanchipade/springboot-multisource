package com.apple.springboot.model;

//import jakarta.persistence.*;
import javax.persistence.*;
import lombok.Getter;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.Type;

import java.time.OffsetDateTime;
import java.util.UUID;

@Getter
@Entity
    @Table(name = "raw_data_store")
public class RawDataStore {

    // Getters and Setters
    @Id
    @GeneratedValue(generator = "UUID")
    @GenericGenerator(
            name = "UUID",
            strategy = "org.hibernate.id.UUIDGenerator"
    )
    @Column(updatable = false, nullable = false)
    private UUID id;

    @Column(name = "source_uri", nullable = false, columnDefinition = "TEXT")
    private String sourceUri;

    @Column(name = "raw_content_text", columnDefinition = "TEXT")
    private String rawContentText;

    //@Lob
    @Column(name = "raw_content_binary", columnDefinition = "bytea")
    private byte[] rawContentBinary;

    @Type(type = "jsonb")
    @Column(name = "source_metadata", columnDefinition = "jsonb")
    private String sourceMetadata; // Store as JSON string

    @CreationTimestamp
    @Column(name = "received_at", nullable = false, updatable = false)
    private OffsetDateTime receivedAt;

    @Column(name = "status", nullable = false, columnDefinition = "TEXT")
    private String status;

    @Column(name = "content_hash", columnDefinition = "TEXT")
    private String contentHash;

    @Column(name = "version")
    private Integer version = 1;

    @Column(name = "latest")
    private Boolean latest = true;

    public RawDataStore() {}

    public void setId(UUID id) { this.id = id; }

    public void setSourceUri(String sourceUri) { this.sourceUri = sourceUri; }

    public void setRawContentText(String rawContentText) { this.rawContentText = rawContentText; }

    public void setRawContentBinary(byte[] rawContentBinary) { this.rawContentBinary = rawContentBinary; }

    public void setSourceMetadata(String sourceMetadata) { this.sourceMetadata = sourceMetadata; }

    public void setReceivedAt(OffsetDateTime receivedAt) { this.receivedAt = receivedAt; }

    public void setStatus(String status) { this.status = status; }

    public String getContentHash() { return contentHash; }

    public void setContentHash(String contentHash) { this.contentHash = contentHash; }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public Boolean isLatest() {
        return latest;
    }

    public void setLatest(Boolean latest) {
        this.latest = latest;
    }
}