import React from 'react';
import ReactMarkdown from 'react-markdown';
import './Message.css';

const Message = ({ sender, text }) => {
  const messageClass = sender === 'USER' ? 'user-message' : 'bot-message';
  return (
    <div className={`message ${messageClass}`}>
      <ReactMarkdown>{text}</ReactMarkdown>
    </div>
  );
};

export default Message;
