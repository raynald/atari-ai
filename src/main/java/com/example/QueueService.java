package com.example;

public interface QueueService {
    public void push(String queueUrl, String messageBody);
    // TODO: return PushResult

    public Message pull(String queueUrl);
    // TODO: ReceiveMessageResult

    public void delete(String queueUrl, String receiptHandle);
    // TODO: return DeleteResult
}