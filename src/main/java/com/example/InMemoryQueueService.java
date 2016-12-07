package com.example;

import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

// TODO: differ invisible message with visible message
// TODO: thread safe

public class InMemoryQueueService implements QueueService {
    private static InMemoryQueueService instance = null;
    private ConcurrentHashMap<String, Deque<Message>> mainQueue;
    private ConcurrentHashMap<String, HashMap<String, Message>> invisibleQueueMap;
    private PriorityBlockingQueue<Message> invisibleQueueHeap;

    private Long delaySeconds = 500L;
    private Comparator<Message> comparator = new Comparator<Message>() {
        @Override
        public int compare(Message o1, Message o2) {
            return o1.getRevival().compareTo(o2.getRevival();
        }
    };

    private InMemoryQueueService() {
        invisibleQueueMap = new ConcurrentHashMap<>();
        mainQueue = new ConcurrentHashMap<>();
        invisibleQueueHeap = new PriorityBlockingQueue<>(10, comparator);
    }

    private long now() {
        return System.currentTimeMillis();
    }

    public void setDelaySeconds(Long time) {
        delaySeconds = time;
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

    protected void purseQueue(String queueUrl) {
        getMainQueue(queueUrl).clear();
        getInvisibleQueueMap(queueUrl).clear();
        invisibleQueueHeap.clear();
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
            nextMessage.setRevival(now() + TimeUnit.SECONDS.toMillis(delaySeconds));
            getInvisibleQueueMap(queueUrl).put(nextMessage.getReceiptHandle(), nextMessage);
            invisibleQueueHeap.add(nextMessage);
        }
        return nextMessage;
    }

    public void delete(String queueUrl, String receiptHandle) {
        HashMap<String, Message> messageQueue = getInvisibleQueueMap(queueUrl);
        messageQueue.remove(receiptHandle);
    }

    protected void clearInsivible() {
        while (invisibleQueueHeap.size() > 0) {
            Message message = invisibleQueueHeap.peek();
            if (message.getRevival() <= now()) {
                invisibleQueueHeap.remove(message);
                getMainQueue(message.getQueue()).offerLast(message);
            } else break;
        }
    }

    private Deque<Message> getMainQueue(String queueUrl) {
        Deque<Message> result = mainQueue.get(queueUrl);
        if (result == null) {
            result = new ConcurrentLinkedDeque<Message>();
            mainQueue.put(queueUrl, result);
        }
        return result;
    }

    private HashMap<String, Message> getInvisibleQueueMap(String queueUrl) {
        HashMap<String, Message> result = invisibleQueueMap.get(queueUrl);
        if (result == null) {
            result = new HashMap<String, Message>();
            invisibleQueueMap.put(queueUrl, result);
        }
        return result;
    }
}
