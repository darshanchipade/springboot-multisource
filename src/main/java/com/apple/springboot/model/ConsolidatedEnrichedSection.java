package com.apple.springboot.model;

import lombok.*;
import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.Type;
import org.hibernate.type.SqlTypes;

import javax.persistence.*;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;


@Entity
@Setter
@Getter
@Table(name = "consolidated_enriched_sections")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConsolidatedEnrichedSection {

    @Id
    @GeneratedValue
    @Column(updatable = false, nullable = false)
    private UUID id;

    @Column(name = "cleansed_data_id", nullable = false)
    private UUID cleansedDataId;

    @Column(name = "original_field_name")
    private String originalFieldName;

   // @Type(type = "jsonb")
   @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "enrichment_metadata", columnDefinition = "jsonb")
    private String enrichmentMetadata;

    @Column(name = "enriched_at")
    private OffsetDateTime enrichedAt;

    @Column(name = "source_uri", columnDefinition = "TEXT")
    private String sourceUri;

    @Column(name = "section_path")
    private String sectionPath;

    @Column(name = "summary", columnDefinition = "TEXT")
    private String summary;

    @Column(name = "classification")
    private String classification;

    @Column(name = "tags", columnDefinition = "TEXT[]")
    private String[] tags;

    @Column(name = "keywords", columnDefinition = "TEXT[]")
    private String[] keywords;

    @Column(name = "cleansed_text", columnDefinition = "TEXT")
    private String cleansedText;

    @Column(name = "sentiment")
    private String sentiment;

    @Column(name = "section_uri", columnDefinition = "TEXT")
    private String sectionUri;

    @Column(name = "model_used")
    private String modelUsed;

    //@Type(type = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "context", columnDefinition = "jsonb")
    private Map<String, Object> context;

    @Column(name = "status")
    private String status;

    @Column(name = "saved_at")
    private OffsetDateTime savedAt;

    @Column(name = "version")
    private Integer version;

    @Column(name = "content_hash")
    private String contentHash;


}
