package com.example;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;

import static org.mockito.Mockito.spy;

public class FileQueueTest {
    private final String QUEUE_NAME = "MyQueue";
    private FileQueueService service;
    private String messageBody;

    @Before
    public void setUp() {
        service = spy(new FileQueueService.class);
        messageBody = String.format("FileQueueTest%s", System.currentTimeMillis());
    }

    @Test
    public void pushTest() throws InterruptedException, IOException {
        service.push(QUEUE_NAME, messageBody);
        assertEquals("The size of message queue is not one!", 1, service.getSize(QUEUE_NAME));
    }

    @Test
    public void pullTest() throws InterruptedException, IOException {
        service.push(QUEUE_NAME, messageBody);
        Message message = service.pull(QUEUE_NAME);
        assertNotNull("Failed to retrieve the message!", message);
        assertEquals("Message is not same!", message.getMessageBody(), messageBody);
    }

    @Test
    public void deleteTest() throws InterruptedException, IOException {
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
