package com.apple.springboot.model;

import jakarta.persistence.*;
import jakarta.persistence.Transient;
import org.hibernate.annotations.Array;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.OffsetDateTime;
import java.util.UUID;

@SqlResultSetMapping(
    name="ContentChunkWithScore",
    entities={
        @EntityResult(
            entityClass=ContentChunk.class,
            fields={
                @FieldResult(name="id", column="id"),
                @FieldResult(name="version", column="row_version"),
                @FieldResult(name="consolidatedEnrichedSection", column="consolidated_enriched_section_id"),
                @FieldResult(name="chunkText", column="chunk_text"),
                @FieldResult(name="createdAt", column="created_at"),
                @FieldResult(name="createdBy", column="created_by"),
                @FieldResult(name="sourceField", column="source_field"),
                @FieldResult(name="sectionPath", column="section_path"),
                @FieldResult(name="vector", column="vector")
            }
        )
    },
    columns={
        @ColumnResult(name="score", type=Float.class)
    }
)
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

    @Column(name = "created_at")
    private OffsetDateTime createdAt;

    @Column(name = "created_by")
    private String createdBy;

    @Column(name = "source_field")
    private String sourceField;

    @Column(name = "section_path")
    private String sectionPath;

    @JdbcTypeCode(SqlTypes.VECTOR)
    @Array(length = 1024)
    @Column(name = "vector", columnDefinition = "vector(1024)")
    private float[] vector;

    @Transient
    private Float score;
}