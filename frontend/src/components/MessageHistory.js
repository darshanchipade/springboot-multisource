import React, { useEffect, useRef } from 'react';
import Message from './Message';

const MessageHistory = ({ messages }) => {
  const messagesEndRef = useRef(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  return (
    <div className="message-history">
      {Array.isArray(messages) && messages.map((msg, index) => (
        <Message key={index} sender={msg.sender} text={msg.message} />
      ))}
      <div ref={messagesEndRef} />
    </div>
  );
};

export default MessageHistory;
