package com.example;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

public class FileQueueService implements QueueService {
    private static FileQueueService instance = null;
    private final String LOCK_DIR = ".lock";
    private final String INVISIBLE_DIR = "invisible";
    private final String MESSAGE_DIR = "messages";

    public static FileQueueService getInstance() {
        if (instance == null) {
            instance = new FileQueueService();
        }
        return instance;
    }

    @Override
    public void push(String queueUrl, String messageBody) throws InterruptedException, IOException {
        String queue = fromUrl(queueUrl);
        File messages = getMessageFile(queue);
        if (!messages.exists()) {
            messages.mkdir();
        }
        File lock = getLockFile(queue);
        long visibleFrom = (delaySeconds != null) ? now() + TimeUnit.SECONDS.toMillis(delaySeconds): 0L;

        lock(lock);
        try (PrintWriter pw = new PrintWriter(new FileWriter(messages, true))) {
            for (String message: messages) {
                pw.println(Record.create(visibleFrom, message));
            }
        } finally {
            unlock(lock);
        }
    }

    public Message pull(String queueUrl) {

    }

    public void delete(String queueUrl, String receiptHandle) {

    }

    private String fromUrl(String queueUrl) {
        // Sanitize the queue name
        return queueUrl;
    }

    private File getLockFile(String queue) {
        return new File(queue + LOCK_DIR);
    }

    private File getMessageFile(String queue) {
        return new File(queue);
    }

    private void lock(File lock) throws InterruptedException {
        while (!lock.mkdir()) {
            Thread.sleep(50);
        }
    }

    private void unlock(File lock) {
        lock.delete();
    }
}
