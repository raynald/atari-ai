package com.example;

import java.util.Deque;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

//TODO: singleton

public class InMemoryQueueService implements QueueService {
    private static InMemoryQueueService instance = null;
    private ConcurrentHashMap<String, Deque<Message>> mainQueue;
    private ConcurrentHashMap<String, HashMap<String, Message>> invisibleQueue;

    private InMemoryQueueService() {
        invisibleQueue = new ConcurrentHashMap<>();
        mainQueue = new ConcurrentHashMap<>();
    }

    public static InMemoryQueueService getInstance() {
        if (instance == null) {
            instance = new InMemoryQueueService();
        }
        return instance;
    }

    public int getSize(String queueUrl) {
        Deque<Message> messageDeque = getMainQueue(queueUrl);
        return messageDeque.size();
    }

    @Override
    public void push(String queueUrl, String messageBody) {
        Deque<Message> messageDeque = getMainQueue(queueUrl);
        Message message = new Message(messageBody);
        messageDeque.offerLast(message);
    }

    public Message pull(String queueUrl) {
        Deque<Message> messageQueue = getMainQueue(queueUrl);
        Message nextMessage = messageQueue.pollFirst();
        if (nextMessage != null) {
            getInvisibleQueue(queueUrl).put(nextMessage.getReceiptHandle(), nextMessage);
        }
        return nextMessage;
    }

    public void delete(String queueUrl, String receiptHandle) {
        HashMap<String, Message> messageQueue = getInvisibleQueue(queueUrl);
        messageQueue.remove(receiptHandle);
    }

    private Deque<Message> getMainQueue(String queueUrl) {
        Deque<Message> result = mainQueue.get(queueUrl);
        if (result == null) {
            result = new ConcurrentLinkedDeque<Message>();
            mainQueue.put(queueUrl, result);
        }
        return result;
    }

    private HashMap<String, Message> getInvisibleQueue(String queueUrl) {
        HashMap<String, Message> result = invisibleQueue.get(queueUrl);
        if (result == null) {
            result = new HashMap<String, Message>();
            invisibleQueue.put(queueUrl, result);
        }
        return result;
    }
}
