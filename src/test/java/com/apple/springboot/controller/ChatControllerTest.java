package com.apple.springboot.controller;

import com.apple.springboot.model.ChatMessage;
import com.apple.springboot.service.InteractiveSearchService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;
import org.springframework.test.web.servlet.MockMvc;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = ChatController.class,
    excludeAutoConfiguration = {DataSourceAutoConfiguration.class, JpaRepositoriesAutoConfiguration.class, HibernateJpaAutoConfiguration.class, BatchAutoConfiguration.class})
public class ChatControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private InteractiveSearchService interactiveSearchService;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    public void testChat() throws Exception {
        UUID conversationId = UUID.randomUUID();
        ChatMessage userMessage = new ChatMessage("Hello", ChatMessage.Sender.USER, conversationId, new ArrayList<>());

        ChatMessage botResponse = new ChatMessage("Hi there! How can I help you?", ChatMessage.Sender.BOT, conversationId, null);

        when(interactiveSearchService.processMessage(any(ChatMessage.class), any(List.class))).thenReturn(botResponse);

        mockMvc.perform(post("/api/chat")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(userMessage)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.message").value("Hi there! How can I help you?"))
                .andExpect(jsonPath("$.sender").value("BOT"));
    }
}
