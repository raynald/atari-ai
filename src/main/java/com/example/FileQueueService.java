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
            Path queueMessagePath = getQueueMessageQueueDir(queueUrl);
            Files.write(queueMessagePath, message.toString().getBytes(), StandardOpenOption.APPEND);
        } finally {
            unlock(lock);
        }
    }

    public Message pull(String queueUrl) throws InterruptedException, IOException {
        final String MESSAGE_SHADOW_NAME = "shadow";

        String queue = fromUrl(queueUrl);
        Path path = getQueueDir(queue);
        File queueMessagePath = getQueueMessageQueueDir(queueUrl).toFile();
        File shadowQueuePath = path.resolve(MESSAGE_SHADOW_NAME).toFile();
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
            Path invisibleQueuePath = getInvisibleQueueDir(queueUrl);
            Files.write(invisibleQueuePath, String.format("%s:%s", message, now() + delaySeconds).getBytes(), StandardOpenOption.APPEND);
            return new Message(message);
        } finally {
            unlock(lock);
        }
    }

    public void delete(String queueUrl, String receiptHandle) throws InterruptedException, IOException {
        File lock = getLockFile(queueUrl);
        try {
            lock(lock);
            List<String> invisibleMessages = Files.readAllLines(getInvisibleQueueDir(queueUrl));
            List<String> afterDeleteMessages = new LinkedList<>();
            for(String message: invisibleMessages) {
                String[] split = message.split(":");
                if (!split[0].equals(receiptHandle)) {
                    afterDeleteMessages.add(message);
                }
            }
            Path invisiblePath = getInvisibleQueueDir(queueUrl);
            for (String message: afterDeleteMessages) {
                Files.write(invisiblePath, message.getBytes(), StandardOpenOption.APPEND);
            }
        } finally {
            unlock(lock);
        }
    }

    public int getSize(String queueUrl) throws IOException {
        return Files.readAllLines(getQueueMessageQueueDir(queueUrl)).size();
    }

    private Path getQueuePath(String queueUrl) {
        return basePath.resolve(queueUrl);
    }

    private Path getInvisibleQueueDir(String queueUrl) throws IOException {
        final String INVISIBLE_FILE_NAME = "invisible";
        return getQueueDir(queueUrl).resolve(INVISIBLE_FILE_NAME);
    }

    private Path getQueueMessageQueueDir(String queueUrl) throws IOException {
        return getQueueDir(queueUrl).resolve(MESSAGE_FILE_NAME);
    }

    private Path getShadowQueueMessageQueueDir(String queueUrl) throws IOException {
        return getQueueDir(queueUrl).resolve(MESSAGE_FILE_NAME);
    }

    protected void clearInsivible(String queueUrl) throws IOException {
        List<String> messages = Files.readAllLines(getInvisibleQueueDir(queueUrl));
        List<String> afterCleanMessgages = new LinkedList<String>();
        Path messageQueuePath = getQueueMessageQueueDir(queueUrl);
        for(String message: messages) {
            String[] split = message.split(":");
            if (Long.valueOf(split[1]) <= now()) {
                Files.write(messageQueuePath, message.getBytes(), StandardOpenOption.APPEND);
            } else {
                afterCleanMessgages.add(split[0]);
            }
        }
        Path invisiblePath = getInvisibleQueueDir(queueUrl);
        for (String message: afterCleanMessgages) {
            Files.write(invisiblePath, message.getBytes(), StandardOpenOption.APPEND);
        }
    }

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

    private String fromUrl(String queueUrl) {
        // Sanitize the queue name
        return queueUrl;
    }

    private File getLockFile(String queue) {
        final String LOCK_DIR = ".lock";
        return new File(queue + LOCK_DIR);
    }

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
