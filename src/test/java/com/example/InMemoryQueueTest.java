package com.example;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.spy;

public class InMemoryQueueTest {
    private final String QUEUE_NAME = "MyQueue";
    private InMemoryQueueService service;
    private String messageBody;

    @Before
    public void setUp() {
        service = spy(InMemoryQueueService.class);
        messageBody = String.format("InMemeryQueueTest%s", System.currentTimeMillis());
    }

    @Test
    public void pushTest() {
        service.push(QUEUE_NAME, messageBody);
        assertEquals("The size of message queue is not one!", 1, service.getSize(QUEUE_NAME));
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
        assertEquals("Queue is not empty!", 0, service.getSize(QUEUE_NAME));
    }

    @Test
    public void revivalTest() {
        service.push(QUEUE_NAME, messageBody);
        Message firstMessage = service.pull(QUEUE_NAME);
        Message secondMessage = service.pull(QUEUE_NAME);
        assertNull("Second pulled message is not NULL!", secondMessage);
    }
}
