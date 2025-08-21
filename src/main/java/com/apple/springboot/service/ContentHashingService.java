package com.apple.springboot.service;

import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@Service
public class ContentHashingService {

    public String calculateContentHash(String content, String context) {
        if (content == null || content.isEmpty()) return null;
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(content.getBytes(StandardCharsets.UTF_8));
            if (context != null && !context.isEmpty()) {
                digest.update(context.getBytes(StandardCharsets.UTF_8));
            }
            byte[] encodedhash = digest.digest();
            return bytesToHex(encodedhash);
        } catch (NoSuchAlgorithmException e) {
            // This should not happen in a standard Java environment
            throw new RuntimeException("SHA-256 algorithm not found", e);
        }
    }

    private String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder(2 * hash.length);
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }
}
