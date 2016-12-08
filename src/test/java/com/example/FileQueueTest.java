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
        messageBody = String.format("InMemeryQueueTest%s", System.currentTimeMillis());
    }

    @Test
    public void pushTest() throws InterruptedException, IOException {
        service.push(QUEUE_NAME, messageBody);
        assertEquals("cannot", )
    }

    @Test
    public void pullTest() throws InterruptedException, IOException {
        service.push(QUEUE_NAME, messageBody);
        Message message = service.pull(QUEUE_NAME);
        assertNotNull("fsf", message);
        assertTrue("fsl", message.getMessageBody().equals(messageBody) == true);
    }

    @Test
    public void deleteTest() throws InterruptedException, IOException {
        service.push(QUEUE_NAME, messageBody);
        Message message = service.pull(QUEUE_NAME);
        service.delete(QUEUE_NAME, message.getReceiptHandle());
        assertEquals("xxx", );
    }

    @Test
    public void revivalTest() {

    }
}
