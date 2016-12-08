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

    private InMemoryQueueService() {
        Comparator<Message> comparator = new Comparator<Message>() {
            @Override
            public int compare(Message o1, Message o2) {
                return o1.getRevival().compareTo(o2.getRevival();
            }
        };
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

    int getQueueSize(String queue) {
        Deque<Message> messageDeque = getMainQueue(queue);
        return messageDeque.size();
    }

    int getInvisibleSize() {
        return invisibleQueueHeap.size();
    }

    /**
     * Purge the queue
     * @param queue queue name
     */
    protected void purgeQueue(String queue) {
        getMainQueue(queue).clear();
        getInvisibleQueueMap(queue).clear();
        invisibleQueueHeap.clear();
    }

    @Override
    public void push(String queueUrl, String messageBody) {
        String queue = fromUrl(queueUrl);
        Deque<Message> messageDeque = getMainQueue(queue);
        Message message = new Message(messageBody);
        messageDeque.offerLast(message);
    }

    @Override
    public Message pull(String queueUrl) {
        String queue = fromUrl(queueUrl);
        Deque<Message> messageQueue = getMainQueue(queue);
        Message nextMessage = messageQueue.pollFirst();
        if (nextMessage != null) {
            nextMessage.setRevival(now() + TimeUnit.SECONDS.toMillis(delaySeconds));
            getInvisibleQueueMap(queue).put(nextMessage.getReceiptHandle(), nextMessage);
            invisibleQueueHeap.add(nextMessage);
        }
        return nextMessage;
    }

    @Override
    public void delete(String queueUrl, String receiptHandle) {
        String queue = fromUrl(queueUrl);
        HashMap<String, Message> messageQueue = getInvisibleQueueMap(queue);
        messageQueue.remove(receiptHandle);
    }

    /**
     * Clear the insivible Heap
     */
    protected void clearInsivible() {
        while (invisibleQueueHeap.size() > 0) {
            Message message = invisibleQueueHeap.peek();
            if (message.getRevival() <= now()) {
                invisibleQueueHeap.remove(message);
                getMainQueue(message.getQueue()).offerLast(message);
            } else break;
        }
    }

    /**
     * Sanitize the queue name
     * @param queueUrl original queue url
     * @return sanitized queue name
     */
    private String fromUrl(String queueUrl) {
        return queueUrl;
    }

    private Deque<Message> getMainQueue(String queue) {
        return mainQueue.computeIfAbsent(queue, k -> new ConcurrentLinkedDeque<Message>());
    }

    private HashMap<String, Message> getInvisibleQueueMap(String queue) {
        return invisibleQueueMap.computeIfAbsent(queue, k -> new HashMap<String, Message>());
    }
}
