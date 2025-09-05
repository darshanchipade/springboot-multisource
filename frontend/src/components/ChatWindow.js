import React, { useState, useEffect } from 'react';
import MessageHistory from './MessageHistory';
import ChatInput from './ChatInput';
import './ChatWindow.css';
import { v4 as uuidv4 } from 'uuid';

const ChatWindow = () => {
  const [messages, setMessages] = useState([]);
  const [conversationId, setConversationId] = useState(null);

  useEffect(() => {
    // Generate a conversation ID when the component mounts
    setConversationId(uuidv4());
  }, []);

  const handleSendMessage = async (text) => {
    if (!text.trim()) return;

    // Create a history for the API call that doesn't have nested histories
    const historyForApi = messages.map(msg => ({
      message: msg.message,
      sender: msg.sender,
      conversationId: msg.conversationId
      // The 'history' property is omitted
    }));

    const userMessage = {
      message: text,
      sender: 'USER',
      conversationId: conversationId,
      history: historyForApi
    };

    // Add user message to the UI immediately
    setMessages(prevMessages => [...prevMessages, userMessage]);

    try {
      const response = await fetch('/api/chat', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(userMessage),
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const botMessage = await response.json();

      // Add bot response to the UI
      setMessages(prevMessages => [...prevMessages, botMessage]);

    } catch (error) {
      console.error("Failed to send message:", error);
      const errorMessage = {
        message: 'Sorry, I failed to connect to the server. Please try again.',
        sender: 'BOT',
        conversationId: conversationId,
        history: null
      };
      setMessages(prevMessages => [...prevMessages, errorMessage]);
    }
  };

  return (
    <div className="chat-window">
      <MessageHistory messages={messages} />
      <ChatInput onSendMessage={handleSendMessage} />
    </div>
  );
};

export default ChatWindow;
