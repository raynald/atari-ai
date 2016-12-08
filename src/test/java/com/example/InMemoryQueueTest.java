package com.example;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class InMemoryQueueTest {
    private final String QUEUE_NAME = "MyQueue";
    private InMemoryQueueService service;
    private String messageBody;

    @Before
    public void setUp() {
        service = InMemoryQueueService.getInstance();
        messageBody = String.format("InMemeryQueueTest%s", System.currentTimeMillis());
        service.purgeQueue(QUEUE_NAME);
    }

    @Test
    public void pushTest() {
        service.push(QUEUE_NAME, messageBody);
        assertEquals("The size of message queue is not one!", 1, service.getQueueSize(QUEUE_NAME));
    }

    @Test
    public void pullTest() {
        service.push(QUEUE_NAME, messageBody);
        Message message = service.pull(QUEUE_NAME);
        assertNotNull("Failed to retrieve the message!", message);
        assertEquals("Message is not same!", message.getMessageBody(), messageBody);
    }

    @Test
    public void deleteTest() {
        service.push(QUEUE_NAME, messageBody);
        Message message = service.pull(QUEUE_NAME);
        service.delete(QUEUE_NAME, message.getReceiptHandle());
        assertEquals("Queue is not empty!", 0, service.getQueueSize(QUEUE_NAME));
    }

    @Test
    public void revivalTest() {
        service.push(QUEUE_NAME, messageBody);
        Message firstMessage = service.pull(QUEUE_NAME);
        Message secondMessage = service.pull(QUEUE_NAME);
        assertNull("Second pulled message is not NULL!", secondMessage);
    }

    @Test
    public void concurrentTest() {
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);
        for (int i = 0; i < 1; i++) {
            service.push(QUEUE_NAME, messageBody);
            Runnable worker = () -> {
                Message message = service.pull(QUEUE_NAME);
                service.delete(QUEUE_NAME, message.getReceiptHandle());
            };
            executor.execute(worker);
        }
        executor.shutdown();
        while (!executor.isTerminated()) {}
        assertEquals("Messages queue is not empty", service.getQueueSize(QUEUE_NAME), 0);
        assertEquals("Pending messages container is not empty", service.getInvisibleSize(), 0);
    }
}
