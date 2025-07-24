package com.apple.springboot.model;

import java.io.Serializable;
import java.util.Objects;

public class ContentHashId implements Serializable {
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
