package com.example;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

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
        LOGGER.info(String.format("Successfully create a new folder? %s", mydir.mkdir()));
        basePath = Paths.get(baseDir);
    }

    public static FileQueueService getInstance() {
        if (instance == null) {
            instance = new FileQueueService();
        }
        return instance;
    }

    private long now() {
        return System.currentTimeMillis();
    }

    public void setDelayMilliSeconds(Long time) {
        delayMilliSeconds = time;
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
            LOGGER.info("Shadow queue length: " + shadowQueuePath.length());
            if (shadowQueuePath.length() == 0) {
                List<String> allMessages = Files.readAllLines(queueMessagePath.toPath());
                for (int i = allMessages.size() - 1;i >= 0;i --) {
                    Files.write(shadowQueuePath.toPath(), String.format("%s\n", allMessages.get(i)).getBytes(), StandardOpenOption.APPEND);
                }
                PrintWriter writer = new PrintWriter(queueMessagePath);
                writer.print("");
                writer.close();
            }
            String rawString = readFromLast(shadowQueuePath);
            LOGGER.info("Raw String: " + rawString);
            message = Message.fromString(rawString);
            Path invisibleQueuePath = getInvisibleQueuePath(queue);
            if (message != null) {
                message.setRevival(now() + delayMilliSeconds);
                Files.write(invisibleQueuePath, String.format("%s\n", message.toString()).getBytes(), StandardOpenOption.APPEND);
                LOGGER.info("Pulled raw message: " + message);
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
            for(String rawMessage: invisibleMessages) {
                Message message = Message.fromString(rawMessage);
                if (message != null && !message.getReceiptHandle().equals(receiptHandle)) {
                    afterDeleteMessages.add(rawMessage);
                }
            }
            Path invisiblePath = getInvisibleQueuePath(queue);
            // Clear file content
            PrintWriter writer = new PrintWriter(invisiblePath.toFile());
            writer.print("");
            writer.close();
            for (String message: afterDeleteMessages) {
                Files.write(invisiblePath, String.format("%s\n", message).getBytes(), StandardOpenOption.APPEND);
            }
        } finally {
            unlock(lock);
        }
    }

    public int getQueueSize(String queue) throws IOException {
        return Files.readAllLines(getQueueMessageQueuePath(queue)).size() +
                Files.readAllLines(getShadowQueueMessageQueuePath(queue)).size();
    }

    public int getInvisibleSize(String queue) throws IOException {
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
     * Purge the queue
     * @param queue queue name
     */
    protected void purgeQueue(String queue) throws IOException {
        getQueueMessageQueuePath(queue).toFile().delete();
        getShadowQueueMessageQueuePath(queue).toFile().delete();
        getInvisibleQueuePath(queue).toFile().delete();
    }

    /**
     * Clear the insivible queue
     * @param queue the name of the queue
     * @throws IOException
     */
    protected void clearInsivible(String queue) throws IOException {
        List<String> messages = Files.readAllLines(getInvisibleQueuePath(queue));
        List<String> afterCleanMessgages = new LinkedList<String>();
        Path shadowQueuePath = getShadowQueueMessageQueuePath(queue);
        for(String rawMessage: messages) {
            Message message = Message.fromString(rawMessage);
            if (message == null) continue;
            if (message.getRevival() <= now()) {
                Files.write(shadowQueuePath, String.format("%s\n", message).getBytes(), StandardOpenOption.APPEND);
            } else {
                afterCleanMessgages.add(rawMessage);
            }
        }
        Path invisiblePath = getInvisibleQueuePath(queue);
        // Clear file content
        PrintWriter writer = new PrintWriter(invisiblePath.toFile());
        writer.print("");
        writer.close();
        for (String message: afterCleanMessgages) {
            Files.write(invisiblePath, String.format("%s\n", message).getBytes(), StandardOpenOption.APPEND);
        }
    }

    /**
     * Read the last line of file and also delete it
     * @param file the file to read
     * @return last line of the file
     * @throws IOException
     */
    private String readFromLast(File file) throws IOException{
        StringBuilder builder = new StringBuilder();
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            LOGGER.info("File length: " + randomAccessFile.length());
            long fileLength = file.length() - 2;
            if (fileLength < 0) return "";
            // Set the pointer at the last of the file
            randomAccessFile.seek(fileLength);
            long pointer;
            for(pointer = fileLength; pointer >= 0; pointer--){
                randomAccessFile.seek(pointer);
                char c;
                // read from the last one char at the time
                c = (char)randomAccessFile.read();
                // break when end of the line
                if(c == '\n'){
                    break;
                }
                builder.append(c);
            }
            // Since line is read from the last so it
            // is in reverse so use reverse method to make it right
            LOGGER.info("file new length: " + String.valueOf(pointer));
            randomAccessFile.getChannel().truncate(pointer + 1);
            builder.reverse();
            LOGGER.info("Get last line: " + builder);
            LOGGER.info("Length after truncate: " + randomAccessFile.length());
            return builder.toString();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally{
            if(randomAccessFile != null){
                try {
                    randomAccessFile.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        return "";
    }

    /**
     * Sanitize the queue name
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
     * Calculate queue directory path
     * @param queue the queue name
     * @return queue directory path
     * @throws IOException
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
        LOGGER.info("After delete: " + String.valueOf(lock.delete()));
    }
}
