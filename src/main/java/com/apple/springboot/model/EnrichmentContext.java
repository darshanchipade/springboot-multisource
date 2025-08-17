
package com.apple.springboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class EnrichmentContext {
    @JsonProperty("envelope")
    private Envelope envelope;

    @JsonProperty("facets")
    private Facets facets;
}
