package com.apple.springboot.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ChatMessage {
    private String message;
    private Sender sender;
    private UUID conversationId;
    private List<ChatMessage> history;

    public enum Sender {
        USER, BOT
    }
}
