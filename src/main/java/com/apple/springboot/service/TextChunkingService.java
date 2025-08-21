package com.apple.springboot.service;

import com.apple.springboot.config.ChunkingConfig;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class TextChunkingService {

//    private static final int CHUNK_SIZE = 256;
//    private static final int CHUNK_OVERLAP = 32;
//
//    public List<String> chunk(String text) {
//        List<String> chunks = new ArrayList<>();
//        if (text == null || text.isEmpty()) {
//            return chunks;
//        }
//
//        for (int i = 0; i < text.length(); i += (CHUNK_SIZE - CHUNK_OVERLAP)) {
//            int end = Math.min(i + CHUNK_SIZE, text.length());
//            chunks.add(text.substring(i, end));
//        }
//        return chunks;
//    }

//    private static final int SENTENCES_PER_CHUNK = 2;  // tune based on your use case
//    private static final int SENTENCE_OVERLAP = 1;
//
//    public List<String> chunk(String text) {
//        List<String> sentences = splitIntoSentences(text);
//        List<String> chunks = new ArrayList<>();
//
//        for (int i = 0; i < sentences.size(); i += (SENTENCES_PER_CHUNK - SENTENCE_OVERLAP)) {
//            StringBuilder chunk = new StringBuilder();
//            for (int j = i; j < i + SENTENCES_PER_CHUNK && j < sentences.size(); j++) {
//                chunk.append(sentences.get(j)).append(" ");
//            }
//            chunks.add(chunk.toString().trim());
//        }
//        return chunks;
//
//    }
//
//    private List<String> splitIntoSentences(String text) {
//        // Simple regex-based sentence splitter
//        String[] rawSentences = text.split("(?<=[.!?])\\s+");
//        List<String> sentences = new ArrayList<>();
//        for (String sentence : rawSentences) {
//            sentence = sentence.trim();
//            if (!sentence.isEmpty()) {
//                sentences.add(sentence);
//            }
//        }
//        return sentences;
//    }

    public List<String> chunkIfNeeded(String text) {
        if (text == null || text.trim().isEmpty()) return List.of();

        if (text.length() <= ChunkingConfig.LENGTH_THRESHOLD) {
            return List.of(text.trim()); // short enough to embed directly
        } else {
            return chunkBySentences(text);
        }
    }

    private List<String> chunkBySentences(String text) {
        String[] rawSentences = text.split("(?<=[.!?])\\s+");
        List<String> sentences = new ArrayList<>();
        for (String sentence : rawSentences) {
            sentence = sentence.trim();
            if (!sentence.isEmpty()) sentences.add(sentence);
        }

        List<String> chunks = new ArrayList<>();
        for (int i = 0; i < sentences.size(); i += (ChunkingConfig.SENTENCES_PER_CHUNK - ChunkingConfig.SENTENCE_OVERLAP)) {
            StringBuilder chunk = new StringBuilder();
            for (int j = i; j < i + ChunkingConfig.SENTENCES_PER_CHUNK && j < sentences.size(); j++) {
                chunk.append(sentences.get(j)).append(" ");
            }
            chunks.add(chunk.toString().trim());
        }
        return chunks;
    }
}