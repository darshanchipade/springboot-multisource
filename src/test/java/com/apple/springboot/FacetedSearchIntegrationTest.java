package com.apple.springboot;

import com.apple.springboot.model.ConsolidatedEnrichedSection;
import com.apple.springboot.model.ContentChunk;
import com.apple.springboot.model.SearchRequest;
import com.apple.springboot.repository.ConsolidatedEnrichedSectionRepository;
import com.apple.springboot.repository.ContentChunkRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
public class FacetedSearchIntegrationTest {

    @Container
    public static PostgreSQLContainer<?> postgreSQLContainer = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("test-db")
            .withUsername("test")
            .withPassword("test");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgreSQLContainer::getJdbcUrl);
        registry.add("spring.datasource.username", postgreSQLContainer::getUsername);
        registry.add("spring.datasource.password", postgreSQLContainer::getPassword);
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        registry.add("spring.jpa.properties.hibernate.dialect", () -> "org.hibernate.dialect.PostgreSQLDialect");
    }


    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ConsolidatedEnrichedSectionRepository sectionRepository;

    @Autowired
    private ContentChunkRepository chunkRepository;

    @BeforeEach
    void setUp() {
        chunkRepository.deleteAll();
        sectionRepository.deleteAll();

        Map<String, Object> envelope = Map.of(
            "pathHierarchy", List.of("", "content", "dam", "education")
        );
        Map<String, Object> context = Map.of(
            "envelope", envelope
        );

        ConsolidatedEnrichedSection section = ConsolidatedEnrichedSection.builder()
                .id(UUID.randomUUID())
                .cleansedText("ipad for education")
                .tags(List.of("ipad", "education"))
                .keywords(List.of("learning", "school"))
                .context(context)
                .build();
        sectionRepository.save(section);

        ContentChunk chunk = ContentChunk.builder()
                .consolidatedEnrichedSection(section)
                .chunkText("A great tool for learning.")
                .build();
        chunkRepository.save(chunk);
    }

    @Test
    public void testSearchWithPathHierarchyFilter_shouldReturnResults() throws Exception {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.setQuery("ipad");
        searchRequest.setContext(Map.of("pathHierarchy", Collections.singletonList("education")));

        mockMvc.perform(post("/api/search")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(searchRequest)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$[0]").exists());
    }
}
