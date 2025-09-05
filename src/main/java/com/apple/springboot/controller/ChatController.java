package com.apple.springboot.controller;

import com.apple.springboot.model.ChatMessage;
import com.apple.springboot.service.InteractiveSearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/api/chat")
public class ChatController {

    private final InteractiveSearchService interactiveSearchService;

    @Autowired
    public ChatController(InteractiveSearchService interactiveSearchService) {
        this.interactiveSearchService = interactiveSearchService;
    }

    @PostMapping
    public ChatMessage chat(@RequestBody ChatMessage userMessage) {
        return interactiveSearchService.processMessage(userMessage, userMessage.getHistory());
    }
}
