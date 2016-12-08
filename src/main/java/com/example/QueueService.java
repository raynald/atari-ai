package com.example;

import java.io.IOException;

public interface QueueService {
    void push(String queueUrl, String messageBody) throws InterruptedException, IOException;
    Message pull(String queueUrl) throws InterruptedException, IOException;
    void delete(String queueUrl, String receiptHandle) throws InterruptedException, IOException;
}
