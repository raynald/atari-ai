package com.example;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class FileQueueTest {
    private final String BASE_QUEUE_NAME = "MyQueue";
    private FileQueueService service;
    private String messageBody;
    private String messageBodyNew;

    @Before
    public void setUp() throws IOException {
        service = FileQueueService.getInstance();
        messageBody = String.format("FileQueueTest%s", System.currentTimeMillis());
        messageBodyNew = String.format("FileQueueTestNew%s", System.currentTimeMillis());
    }

    @Test
    public void pushTest() throws InterruptedException, IOException {
        String QUEUE_NAME = BASE_QUEUE_NAME + "Push";
        service.purgeQueue(QUEUE_NAME);

        service.push(QUEUE_NAME, messageBody);
        assertEquals("The size of message queue is not one!", 1, service.getQueueSize(QUEUE_NAME));
    }

    @Test
    public void pullTest() throws InterruptedException, IOException {
        String QUEUE_NAME = BASE_QUEUE_NAME + "Pull";
        service.purgeQueue(QUEUE_NAME);
        service.push(QUEUE_NAME, messageBody);
        Message message = service.pull(QUEUE_NAME);
        assertNotNull("Failed to retrieve the message!", message);
        assertEquals("Message is not same!", messageBody, message.getMessageBody());
    }

    @Test
    public void deleteTest() throws InterruptedException, IOException {
        String QUEUE_NAME = BASE_QUEUE_NAME + "Delete";
        service.purgeQueue(QUEUE_NAME);
        service.push(QUEUE_NAME, messageBody);
        Message message = service.pull(QUEUE_NAME);
        service.delete(QUEUE_NAME, message.getReceiptHandle());
        assertEquals("Queue is not empty!", 0, service.getQueueSize(QUEUE_NAME));
    }

    @Test
    public void revivalTest() throws InterruptedException, IOException{
        String QUEUE_NAME = BASE_QUEUE_NAME + "Revival";
        service.purgeQueue(QUEUE_NAME);
        service.push(QUEUE_NAME, messageBody);
        Message firstMessage = service.pull(QUEUE_NAME);
        Message secondMessage = service.pull(QUEUE_NAME);
        assertNull("Second pulled message is not NULL!", secondMessage);
    }

    @Test(timeout = 1000)
    public void timeoutTest() throws InterruptedException, IOException{
        String QUEUE_NAME = BASE_QUEUE_NAME + "Timeout";
        service.purgeQueue(QUEUE_NAME);
        service.setDelayMilliSeconds(0L);
        service.push(QUEUE_NAME, messageBody);
        service.push(QUEUE_NAME, messageBodyNew);
        assertEquals(2, service.getQueueSize(QUEUE_NAME));
        assertEquals(messageBody, service.pull(QUEUE_NAME).getMessageBody());
        assertEquals(1, service.getQueueSize(QUEUE_NAME));
        while(service.getInvisibleSize(QUEUE_NAME) != 0 || service.getQueueSize(QUEUE_NAME) != 2)  {
            service.clearInsivible(QUEUE_NAME);
        }
        assertEquals(0, service.getInvisibleSize(QUEUE_NAME));
        assertEquals(2, service.getQueueSize(QUEUE_NAME));
        assertEquals(messageBody, service.pull(QUEUE_NAME).getMessageBody());
    }

    @Test
    public void concurrentTest() throws InterruptedException, IOException {
        String QUEUE_NAME = BASE_QUEUE_NAME + "Concur";
        service.purgeQueue(QUEUE_NAME);
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);
        for (int i = 0; i < 1; i++) {
            service.push(QUEUE_NAME, messageBody);
            Runnable worker = () -> {
                try {
                    Message message = service.pull(QUEUE_NAME);
                    service.delete(QUEUE_NAME, message.getReceiptHandle());
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            };
            executor.execute(worker);
        }
        executor.shutdown();
        while (!executor.isTerminated()) {}
        assertEquals("Messages queue is not empty", 0, service.getQueueSize(QUEUE_NAME));
        assertEquals("Pending messages container is not empty", 0, service.getInvisibleSize(QUEUE_NAME));
    }

}
