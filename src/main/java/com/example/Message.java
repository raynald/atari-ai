package com.example;

import java.util.UUID;

public class Message {
    private String messageBody;
    private String receiptHandle;

    public Message(String body) {
        messageBody = body;
        receiptHandle = UUID.randomUUID().toString();
    }

    public Message(String body, String handle) {
        messageBody = body;
        receiptHandle = handle;
    }

    public String getMessageBody() {
        return messageBody;
    }

    public String getReceiptHandle() {
        return receiptHandle;
    }
}
