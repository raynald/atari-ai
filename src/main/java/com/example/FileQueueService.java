package com.example;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

/**
 * File implementation of queue service.
 * Each queue is a folder with message stores in a file 'messages' and 'shadow'.
 * Early messages are stored in shadow with reversing order (First message is at the bottom of the file).
 * This design prevents rewriting the file after consuming a message. Invisible messages are stored in
 * file 'invisible' and the folder '.lock' is served as a mutex lock as a concurrency solution.
 */
public class FileQueueService implements QueueService {
    private static final Logger LOGGER = Logger.getLogger(FileQueueService.class.getName());
    private static FileQueueService instance = null;
    private Long delayMilliSeconds = 500L;
    private Path basePath;

    private FileQueueService() {
        String baseDir = "tmp";
        String dir = System.getProperty("directory");
        if (dir != null) {
            baseDir = dir;
        }
        File mydir = new File(baseDir);
        mydir.delete();
        mydir.mkdir();
        basePath = Paths.get(baseDir);
    }

    /**
     * Singleton usage of the class.
     * @return instance
     */
    public static FileQueueService getInstance() {
        if (instance == null) {
            instance = new FileQueueService();
        }
        return instance;
    }

    private long now() {
        return System.currentTimeMillis();
    }

    void setDelayMilliSeconds(Long delayTime) {
        delayMilliSeconds = delayTime;
    }

    @Override
    public void push(String queueUrl, String messageBody) throws InterruptedException, IOException {
        String queue = fromUrl(queueUrl);
        Message message = new Message(messageBody);
        File lock = getLockFile(queue);
        try {
            lock(lock);
            Path queueMessagePath = getQueueMessageQueuePath(queue);
            Files.write(queueMessagePath, String.format("%s\n", message.toString()).getBytes(), StandardOpenOption.APPEND);
        } finally {
            unlock(lock);
        }
    }

    @Override
    public Message pull(String queueUrl) throws InterruptedException, IOException {
        String queue = fromUrl(queueUrl);
        File queueMessagePath = getQueueMessageQueuePath(queue).toFile();
        File shadowQueuePath = getShadowQueueMessageQueuePath(queue).toFile();
        File lock = getLockFile(queue);
        Message message;
        try {
            lock(lock);
            if (shadowQueuePath.length() == 0) {
                List<String> allMessages = Files.readAllLines(queueMessagePath.toPath());
                // Reverse the messages and write into shadow queue
                for (int i = allMessages.size() - 1;i >= 0;i --) {
                    Files.write(shadowQueuePath.toPath(), String.format("%s\n", allMessages.get(i)).getBytes(), StandardOpenOption.APPEND);
                }
                // Clear the message file
                PrintWriter writer = new PrintWriter(queueMessagePath);
                writer.print("");
                writer.close();
            }
            // Read the first message in the queue (the last line of the file)
            String rawString = readFromLast(shadowQueuePath);
            LOGGER.info("Raw String: " + rawString);
            message = Message.fromString(rawString);
            Path invisibleQueuePath = getInvisibleQueuePath(queue);
            if (message != null) {
                // Set the revival time and put the message temporarily into the invisible queue
                message.setRevival(now() + delayMilliSeconds);
                Files.write(invisibleQueuePath, String.format("%s\n", message.toString()).getBytes(), StandardOpenOption.APPEND);
            }
        } finally {
            unlock(lock);
        }
        return message;
    }

    @Override
    public void delete(String queueUrl, String receiptHandle) throws InterruptedException, IOException {
        String queue = fromUrl(queueUrl);
        File lock = getLockFile(queue);
        try {
            lock(lock);
            List<String> invisibleMessages = Files.readAllLines(getInvisibleQueuePath(queue));
            List<String> afterDeleteMessages = new LinkedList<>();
            // Iterate through the invisible queue and delete the corresponding message
            for (String rawMessage: invisibleMessages) {
                Message message = Message.fromString(rawMessage);
                if (message != null && !message.getReceiptHandle().equals(receiptHandle)) {
                    afterDeleteMessages.add(rawMessage);
                }
            }
            generateNewInvisibleQueue(queue, afterDeleteMessages);
        } finally {
            unlock(lock);
        }
    }

    /**
     * Help function to generate a new invisible queue.
     * @param queue queue name
     * @param afterDeleteMessages the messages to be put on the queue
     * @throws IOException exception
     */
    private void generateNewInvisibleQueue(String queue, List<String> afterDeleteMessages) throws IOException {
        Path invisiblePath = getInvisibleQueuePath(queue);
        // Clear file content
        PrintWriter writer = new PrintWriter(invisiblePath.toFile());
        writer.print("");
        writer.close();
        // Write the content
        for (String message: afterDeleteMessages) {
            Files.write(invisiblePath, String.format("%s\n", message).getBytes(), StandardOpenOption.APPEND);
        }
    }

    /**
     * The number of visible messages.
     * @param queue queue name
     * @return number of lines in 'messages' and 'shadow'
     * @throws IOException exception
     */
    int getQueueSize(String queue) throws IOException {
        return Files.readAllLines(getQueueMessageQueuePath(queue)).size()
                + Files.readAllLines(getShadowQueueMessageQueuePath(queue)).size();
    }

    /**
     * The number of invisible messages.
     * @param queue queue name
     * @return number of lines in 'invisible'
     * @throws IOException exception
     */
    int getInvisibleSize(String queue) throws IOException {
        return Files.readAllLines(getInvisibleQueuePath(queue)).size();
    }

    private Path getInvisibleQueuePath(String queue) throws IOException {
        final String INVISIBLE_FILE_NAME = "invisible";
        Path res = getQueueDir(queue).resolve(INVISIBLE_FILE_NAME);
        if (Files.notExists(res)) {
            Files.createFile(res);
        }
        return res;
    }

    private Path getQueueMessageQueuePath(String queue) throws IOException {
        final String MESSAGE_FILE_NAME = "messages";

        Path res = getQueueDir(queue).resolve(MESSAGE_FILE_NAME);
        if (Files.notExists(res)) {
            Files.createFile(res);
        }
        return res;
    }

    private Path getShadowQueueMessageQueuePath(String queue) throws IOException {
        final String MESSAGE_SHADOW_NAME = "shadow";

        Path res = getQueueDir(queue).resolve(MESSAGE_SHADOW_NAME);
        if (Files.notExists(res)) {
            Files.createFile(res);
        }
        return res;
    }

    /**
     *  Purge the queue.
     * @param queue queue name
     */
    protected void purgeQueue(String queue) throws IOException {
        getQueueMessageQueuePath(queue).toFile().delete();
        getShadowQueueMessageQueuePath(queue).toFile().delete();
        getInvisibleQueuePath(queue).toFile().delete();
    }

    /**
     * Clear the insivible queue.
     * @param queue the name of the queue
     * @throws IOException exception
     */
    protected void clearInvisible(String queue) throws IOException {
        List<String> messages = Files.readAllLines(getInvisibleQueuePath(queue));
        List<String> afterDeleteMessages = new LinkedList<String>();
        Path shadowQueuePath = getShadowQueueMessageQueuePath(queue);
        for (String rawMessage: messages) {
            Message message = Message.fromString(rawMessage);
            if (message == null) {
                continue;
            }
            // Put the timeout message back to the shadow queue
            if (message.getRevival() <= now()) {
                Files.write(shadowQueuePath, String.format("%s\n", message).getBytes(), StandardOpenOption.APPEND);
            } else {
                afterDeleteMessages.add(rawMessage);
            }
        }
        generateNewInvisibleQueue(queue, afterDeleteMessages);
    }

    /**
     * Read the last line of file and also delete it.
     * @param file the file to read
     * @return last line of the file
     * @throws IOException exception
     */
    private String readFromLast(File file) throws IOException {
        StringBuilder builder = new StringBuilder();
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            LOGGER.info("File length: " + randomAccessFile.length());
            long fileLength = file.length() - 2;
            if (fileLength < 0) {
                return "";
            }
            // Set the pointer to the end of the file.
            randomAccessFile.seek(fileLength);
            long pointer;
            for (pointer = fileLength; pointer >= 0; pointer--) {
                randomAccessFile.seek(pointer);
                char singleChar;
                // Read from the last one char at the time.
                singleChar = (char)randomAccessFile.read();
                // Break when find the end of the line.
                if (singleChar == '\n') {
                    break;
                }
                builder.append(singleChar);
            }
            /* Since line is read from the last so it
             * is in reverse so use reverse method to make it right
             */
            randomAccessFile.getChannel().truncate(pointer + 1);
            builder.reverse();
            return builder.toString();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (randomAccessFile != null) {
                try {
                    randomAccessFile.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return "";
    }

    /**
     * Sanitize the queue name.
     * @param queueUrl original queue url
     * @return sanitized queue name
     */
    private String fromUrl(String queueUrl) {
        return queueUrl;
    }

    private File getLockFile(String queue) throws IOException {
        final String LOCK_DIR = ".lock";

        Path res = getQueueDir(queue).resolve(LOCK_DIR);
        return res.toFile();
    }

    /**
     * Calculate queue directory path.
     * @param queue the queue name
     * @return queue directory path
     * @throws IOException exception
     */
    private Path getQueueDir(String queue) throws IOException {
        Path queueDir = basePath.resolve(queue);
        if (Files.notExists(queueDir)) {
            Files.createDirectories(queueDir);
        }
        return queueDir;
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
