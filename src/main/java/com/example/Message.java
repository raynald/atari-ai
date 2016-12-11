package com.example;

import java.util.UUID;

public class Message {
    final static private String messageSeparator = "@";

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

    public Message(String body, String handle, Long rev) {
        messageBody = body;
        receiptHandle = handle;
        revival = rev;
        queue = "";
    }

    public String toString() {
        return String.format("%s%s%s%s%s", messageBody, messageSeparator
                , receiptHandle, messageSeparator, revival);
    }

    public static Message fromString(String rawString) {
        String[] temps = rawString.split(messageSeparator);
        if (temps.length < 3) return null;
        return new Message(temps[0], temps[1], Long.parseLong(temps[2]));
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
