package com.example;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.List;

public class FileQueueService implements QueueService {
    private static FileQueueService instance = null;
    private final String MESSAGE_FILE_NAME = "messages";
    private Long delaySeconds = 500L;
    private Path basePath;

    private FileQueueService() {
        String baseDir = "tmp";

        String dir = System.getProperty("directory");
        if (dir != null) {
            baseDir = dir;
        }
        File mydir = new File(baseDir);
        if (!mydir.exists()) {
            mydir.mkdir();
        }
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

    public void setDelaySeconds(Long time) {
        delaySeconds = time;
    }

    @Override
    public void push(String queueUrl, String messageBody) throws InterruptedException, IOException {
        String queue = fromUrl(queueUrl);
        Message message = new Message(messageBody);
        Path path = getQueueDir(queue);
        File lock = getLockFile(queue);
        try {
            lock(lock);
            Path queueMessagePath = getQueueMessageQueueDir(queue);
            Files.write(queueMessagePath, message.toString().getBytes(), StandardOpenOption.APPEND);
        } finally {
            unlock(lock);
        }
    }

    @Override
    public Message pull(String queueUrl) throws InterruptedException, IOException {
        final String MESSAGE_SHADOW_NAME = "shadow";

        String queue = fromUrl(queueUrl);
        File queueMessagePath = getQueueMessageQueueDir(queue).toFile();
        File shadowQueuePath = getShadowQueueMessageQueueDir(queue).toFile();
        File lock = getLockFile(queue);
        try {
            lock(lock);
            if (shadowQueuePath.length() == 0) {
                List<String> allMessages = Files.readAllLines(queueMessagePath.toPath());
                for (int i = allMessages.size() - 1;i >= 0;i --) {
                    Files.write(shadowQueuePath.toPath(), allMessages.get(i).getBytes(), StandardOpenOption.APPEND);
                }
                PrintWriter writer = new PrintWriter(queueMessagePath);
                writer.print("");
                writer.close();
            }
            String message = readFromLast(shadowQueuePath);
            Path invisibleQueuePath = getInvisibleQueueDir(queue);
            Files.write(invisibleQueuePath, String.format("%s:%s", message, now() + delaySeconds).getBytes(), StandardOpenOption.APPEND);
            return new Message(message);
        } finally {
            unlock(lock);
        }
    }

    @Override
    public void delete(String queueUrl, String receiptHandle) throws InterruptedException, IOException {
        String queue = fromUrl(queueUrl);

        File lock = getLockFile(queue);
        try {
            lock(lock);
            List<String> invisibleMessages = Files.readAllLines(getInvisibleQueueDir(queue));
            List<String> afterDeleteMessages = new LinkedList<>();
            for(String message: invisibleMessages) {
                String[] split = message.split(":");
                if (!split[0].equals(receiptHandle)) {
                    afterDeleteMessages.add(message);
                }
            }
            Path invisiblePath = getInvisibleQueueDir(queue);
            for (String message: afterDeleteMessages) {
                Files.write(invisiblePath, message.getBytes(), StandardOpenOption.APPEND);
            }
        } finally {
            unlock(lock);
        }
    }

    public int getSize(String queue) throws IOException {
        return Files.readAllLines(getQueueMessageQueueDir(queue)).size();
    }

    private Path getInvisibleQueueDir(String queue) throws IOException {
        final String INVISIBLE_FILE_NAME = "invisible";
        return getQueueDir(queue).resolve(INVISIBLE_FILE_NAME);
    }

    private Path getQueueMessageQueueDir(String queue) throws IOException {
        return getQueueDir(queue).resolve(MESSAGE_FILE_NAME);
    }

    private Path getShadowQueueMessageQueueDir(String queue) throws IOException {
        return getQueueDir(queue).resolve(MESSAGE_FILE_NAME);
    }

    /**
     * Clear the insivible queue
     * @param queue the name of the queue
     * @throws IOException
     */
    protected void clearInsivible(String queue) throws IOException {
        List<String> messages = Files.readAllLines(getInvisibleQueueDir(queue));
        List<String> afterCleanMessgages = new LinkedList<String>();
        Path messageQueuePath = getQueueMessageQueueDir(queue);
        for(String message: messages) {
            String[] split = message.split(":");
            if (Long.valueOf(split[1]) <= now()) {
                Files.write(messageQueuePath, message.getBytes(), StandardOpenOption.APPEND);
            } else {
                afterCleanMessgages.add(split[0]);
            }
        }
        Path invisiblePath = getInvisibleQueueDir(queue);
        for (String message: afterCleanMessgages) {
            Files.write(invisiblePath, message.getBytes(), StandardOpenOption.APPEND);
        }
    }

    /**
     * Read the last line of file and also delete it
     * @param file the file to read
     * @return last line of the file
     * @throws IOException
     */
    private String readFromLast(File file) throws IOException{
        int lines = 0;
        StringBuilder builder = new StringBuilder();
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "r");
            long fileLength = file.length() - 1;
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
            randomAccessFile.setLength(pointer);
            builder.reverse();
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

    private File getLockFile(String queue) {
        final String LOCK_DIR = ".lock";
        return new File(queue + LOCK_DIR);
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
        lock.delete();
    }
}
