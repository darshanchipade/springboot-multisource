package com.apple.springboot.model;

import lombok.Getter;
import lombok.Setter;

import jakarta.persistence.*;
import org.hibernate.annotations.UuidGenerator;

import java.io.Serializable;
import java.util.Objects;

@Setter
@Getter
@Entity
@Table(name = "content_hashes")
@IdClass(ContentHashId.class)
public class ContentHash {

    @Id
    @UuidGenerator
    @Column(name = "source_path", nullable = false, columnDefinition = "TEXT")
    private String sourcePath;

    @Id
    @Column(name = "item_type", nullable = false, columnDefinition = "TEXT")
    private String itemType;

    @Column(name = "content_hash", nullable = false, columnDefinition = "TEXT")
    private String contentHash;

    @Column(name = "context_hash", nullable = true, columnDefinition = "TEXT")
    private String contextHash;

    public ContentHash() {
    }

    public ContentHash(String sourcePath, String itemType, String contentHash, String contextHash) {
        this.sourcePath = sourcePath;
        this.itemType = itemType;
        this.contentHash = contentHash;
        this.contextHash = contextHash;
    }

}
