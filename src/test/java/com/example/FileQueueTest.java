package com.example;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class FileQueueTest {
    private final String QUEUE_NAME = "MyQueue";
    private FileQueueService service;
    private String messageBody;
    private String messageBodyNew;

    @Before
    public void setUp() throws IOException {
        service = FileQueueService.getInstance();
        messageBody = String.format("FileQueueTest%s", System.currentTimeMillis());
        messageBodyNew = String.format("FileQueueTestNew%s", System.currentTimeMillis());
        service.purgeQueue(QUEUE_NAME);
    }

    @Test
    public void pushTest() throws InterruptedException, IOException {
        service.push(QUEUE_NAME, messageBody);
        assertEquals("The size of message queue is not one!", 1, service.getQueueSize(QUEUE_NAME));
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
        assertEquals("Queue is not empty!", 0, service.getQueueSize(QUEUE_NAME));
    }

    @Test
    public void revivalTest() throws InterruptedException, IOException{
        service.push(QUEUE_NAME, messageBody);
        Message firstMessage = service.pull(QUEUE_NAME);
        Message secondMessage = service.pull(QUEUE_NAME);
        assertNull("Second pulled message is not NULL!", secondMessage);
    }

    @Test(timeout = 1000)
    public void timeoutTest() throws InterruptedException, IOException{
        service.setDelaySeconds(0L);
        service.push(QUEUE_NAME, messageBody);
        service.push(QUEUE_NAME, messageBodyNew);

        assertEquals(service.pull(QUEUE_NAME).getMessageBody(), messageBody);
        while(service.getInvisibleSize(QUEUE_NAME) != 0 && service.getQueueSize(QUEUE_NAME) != 2){}

        assertEquals(service.getQueueSize(QUEUE_NAME), 2);
        assertEquals(service.getInvisibleSize(QUEUE_NAME), 0);
        assertEquals(service.pull(QUEUE_NAME).getMessageBody(), messageBody);
    }

    @Test
    public void concurrentTest() throws InterruptedException, IOException {
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
        assertEquals("Messages queue is not empty", service.getQueueSize(QUEUE_NAME), 0);
        assertEquals("Pending messages container is not empty", service.getInvisibleSize(QUEUE_NAME), 0);
    }

}
