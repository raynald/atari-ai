package com.example;

import java.util.Deque;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

//TODO: singleton

public class InMemoryQueueService implements QueueService {
    private ConcurrentHashMap<String, Deque<Message>> mainQueue;
    private ConcurrentHashMap<String, HashMap<String, Message>> invisibleQueue;

    public InMemoryQueueService() {
        invisibleQueue = new ConcurrentHashMap<>();
        mainQueue = new ConcurrentHashMap<>();
    }

    @Override
    public void push(String queueUrl, String messageBody) {
        Deque<Message> messageDeque = mainQueue.get(queueUrl);
        Message message = new Message(messageBody);
        messageDeque.addLast(message);
    }

    public Message pull(String queueUrl) {
        Deque<Message> messageQueue = mainQueue.get(queueUrl);
        Message nextMessage = messageQueue.peek();
        if (nextMessage != null) {
            return nextMessage;
        }
    }

    public void delete(String queueUrl, String receiptHandle) {
        HashMap<String, Message> messageQueue = invisibleQueue.get(queueUrl);
        messageQueue.remove(receiptHandle);
    }
}
