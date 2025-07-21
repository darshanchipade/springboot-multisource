package com.apple.springboot.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Objects;

@Setter
@Getter
@Entity
@Table(name = "content_hashes")
@IdClass(ContentHash.ContentHashId.class)
public class ContentHash {

    @Id
    @Column(name = "source_path", nullable = false, columnDefinition = "TEXT")
    private String sourcePath;

    @Id
    @Column(name = "item_type", nullable = false, columnDefinition = "TEXT")
    private String itemType;

    @Column(name = "content_hash", nullable = false, columnDefinition = "TEXT")
    private String contentHash;

    public ContentHash() {
    }

    public ContentHash(String sourcePath, String itemType, String contentHash) {
        this.sourcePath = sourcePath;
        this.itemType = itemType;
        this.contentHash = contentHash;
    }

    public static class ContentHashId implements Serializable {
        private String sourcePath;
        private String itemType;

        public ContentHashId() {
        }

        public ContentHashId(String sourcePath, String itemType) {
            this.sourcePath = sourcePath;
            this.itemType = itemType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ContentHashId that = (ContentHashId) o;
            return Objects.equals(sourcePath, that.sourcePath) && Objects.equals(itemType, that.itemType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourcePath, itemType);
        }
    }
}
