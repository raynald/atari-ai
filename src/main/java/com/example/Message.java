package com.example;

import java.util.UUID;

public class Message {
    private String messageBody;
    private String receiptHandle;
    private Long revival;
    private String queue;

    public Message(String body) {
        messageBody = body;
        receiptHandle = UUID.randomUUID().toString();
        revival = -1L;
        queue = "";
    }

    public Message(String body, String handle) {
        messageBody = body;
        receiptHandle = handle;
        revival = -1L;
        queue = "";
    }

    public String getMessageBody() {
        return messageBody;
    }

    public String getReceiptHandle() {
        return receiptHandle;
    }

    public Long getRevival() {
        return revival;
    }

    public void setRevival(Long timeout) {
        revival = timeout;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queueName) {
        queue = queueName;
    }
}
