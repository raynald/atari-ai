package com.example;

import com.amazonaws.services.sqs.AmazonSQSClient;

public class SqsQueueService implements QueueService {
    private AmazonSQSClient amazonSQSClient;

    public SqsQueueService(AmazonSQSClient sqsClient) {
        amazonSQSClient = sqsClient;
    }

    @Override
    public void push(String queueUrl, String messageBody) {
        amazonSQSClient.sendMessage(queueUrl, messageBody);
    }

    @Override
    public Message pull(String queueUrl) {
        amazonSQSClient.receiveMessage(queueUrl);
    }

    @Override
    public void delete(String queueUrl, String messageBody) {
        amazonSQSClient.deleteMessage(queueUrl, messageBody);
    }
}
