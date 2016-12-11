package com.example;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class InMemoryQueueTest {
    private final String BASE_QUEUE_NAME = "InMemoryQueue";
    private InMemoryQueueService service;
    private String messageBody;
    private String messageBodyNew;

    @Before
    public void setUp() {
        service = InMemoryQueueService.getInstance();
        messageBody = String.format("InMemeryQueueTest%s", System.currentTimeMillis());
        messageBodyNew = String.format("InMemeryQueueTestNew%s", System.currentTimeMillis());
    }

    @Test
    public void pushTest() {
        String QUEUE_NAME = BASE_QUEUE_NAME + "Push";
        service.purgeQueue(QUEUE_NAME);
        service.push(QUEUE_NAME, messageBody);
        assertEquals("The size of message queue is not one!", 1, service.getQueueSize(QUEUE_NAME));
    }

    @Test
    public void pullTest() {
        String QUEUE_NAME = BASE_QUEUE_NAME + "Pull";
        service.purgeQueue(QUEUE_NAME);
        service.push(QUEUE_NAME, messageBody);
        Message message = service.pull(QUEUE_NAME);
        assertNotNull("Failed to retrieve the message!", message);
        assertEquals("Message is not same!", message.getMessageBody(), messageBody);
    }

    @Test
    public void deleteTest() {
        String QUEUE_NAME = BASE_QUEUE_NAME + "Delete";
        service.purgeQueue(QUEUE_NAME);
        service.push(QUEUE_NAME, messageBody);
        Message message = service.pull(QUEUE_NAME);
        service.delete(QUEUE_NAME, message.getReceiptHandle());
        assertEquals("Queue is not empty!", 0, service.getQueueSize(QUEUE_NAME));
    }

    @Test
    public void revivalTest() {
        String QUEUE_NAME = BASE_QUEUE_NAME + "Revival";
        service.purgeQueue(QUEUE_NAME);
        service.push(QUEUE_NAME, messageBody);
        Message firstMessage = service.pull(QUEUE_NAME);
        Message secondMessage = service.pull(QUEUE_NAME);
        assertNull("Second pulled message is not NULL!", secondMessage);
    }

    @Test(timeout = 1000)
    public void timeoutTest() {
        String QUEUE_NAME = BASE_QUEUE_NAME + "Timeout";
        service.purgeQueue(QUEUE_NAME);
        service.setDelayMilliSeconds(0L);
        service.push(QUEUE_NAME, messageBody);
        service.push(QUEUE_NAME, messageBodyNew);
        assertEquals(2, service.getQueueSize(QUEUE_NAME));
        assertEquals(messageBody, service.pull(QUEUE_NAME).getMessageBody());
        assertEquals(1, service.getQueueSize(QUEUE_NAME));
        while(service.getInvisibleSize(QUEUE_NAME) != 0 || service.getQueueSize(QUEUE_NAME) != 2)  {
            service.clearInvisible();
        }
        assertEquals(0, service.getInvisibleSize(QUEUE_NAME));
        assertEquals(2, service.getQueueSize(QUEUE_NAME));
        assertEquals(messageBody, service.pull(QUEUE_NAME).getMessageBody());
    }

    @Test
    public void concurrentTest() {
        String QUEUE_NAME = BASE_QUEUE_NAME + "Concur";
        service.purgeQueue(QUEUE_NAME);
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
        assertEquals("Messages queue is not empty", 0, service.getQueueSize(QUEUE_NAME));
        assertEquals("Pending messages container is not empty", 0, service.getInvisibleSize(QUEUE_NAME));
    }
}
