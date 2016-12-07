package com.example;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
        assertEquals("xxxx", 1, service.getSize(QUEUE_NAME));
    }

    @Test
    public void pullTest() {
        service.push(QUEUE_NAME, messageBody);
        Message message = service.pull(QUEUE_NAME);
        assertNotNull("hallo", message);
        assertEquals("ree", message.getMessageBody(), messageBody);
    }

    @Test
    public void deleteTest() {
        service.push(QUEUE_NAME, messageBody);
        Message message = service.pull(QUEUE_NAME);
        service.delete(QUEUE_NAME, message.getReceiptHandle());
        assertEquals("caaf", 0, service.getSize());
    }

    @Test
    public void timeoutTest() {

    }
}
