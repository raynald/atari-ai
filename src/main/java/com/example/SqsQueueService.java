package com.example;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import java.util.List;

/**
 * An adapter for Amazon SQS service.
 */
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
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
        receiveMessageRequest.setMaxNumberOfMessages(1);
        ReceiveMessageResult receiveMessageResult = amazonSQSClient.receiveMessage(receiveMessageRequest);
        List<com.amazonaws.services.sqs.model.Message> messages = receiveMessageResult.getMessages();
        if (messages.size() > 0) {
            return new Message(messages.get(0).getBody(), messages.get(0).getReceiptHandle());
        }
        return null;
    }

    @Override
    public void delete(String queueUrl, String messageBody) {
        amazonSQSClient.deleteMessage(queueUrl, messageBody);
    }
}
