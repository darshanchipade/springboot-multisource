package com.apple.springboot.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = false)
public class Facets extends HashMap<String, Object> {

    public Facets() {
        super();
    }

    public Facets(Facets other) {
        super(other);
    }
    public void setSectionName(String sectionName) {
        if (sectionName != null) put("sectionName", sectionName);
    }

    public void setSectionIndex(String sectionIndex) {
        if (sectionIndex != null) put("sectionIndex", sectionIndex);
    }

    public void setSectionPath(String sectionPath) {
        if (sectionPath != null) put("sectionPath", sectionPath);
    }

    public void setSectionModel(String sectionModel) {
        if (sectionModel != null) put("sectionModel", sectionModel);
    }

    public void applyTo(Map<String, String> map) {
        map.putAll();
    }
}
