package com.apple.springboot.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "content_chunks")
public class ContentChunk {

    @Id
    @GeneratedValue
    private UUID id;

    @Version
    @Column(name = "row_version", nullable = false)
    private long version;

    @ManyToOne
    @JoinColumn(name = "consolidated_enriched_section_id", nullable = false)
    private ConsolidatedEnrichedSection consolidatedEnrichedSection;

    @Column(name = "chunk_text", columnDefinition = "TEXT")
    private String chunkText;

    private OffsetDateTime createdAt;

    private String createdBy;

    @Column(name = "source_field")
    private String sourceField;

    @Column(name = "section_path")
    private String sectionPath;

    @Column(name = "vector", columnDefinition = "vector(1024)")
    private float[] vector;
}