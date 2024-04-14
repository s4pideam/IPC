package org.betriebssysteme.Plain.Threaded;

import org.betriebssysteme.Record.Offsets;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ThreadServer {
    private RandomAccessFile randomAccessFile = null;
    public ConcurrentHashMap<String, ThreadClient> clientQueue = new ConcurrentHashMap<>();
    public ConcurrentHashMap<String, Thread> clientThreads = new ConcurrentHashMap<>();
    public List<List<String>> alphabetSplit = new ArrayList<>();
    public ConcurrentHashMap<String, Integer> wordCount = new ConcurrentHashMap<>();
    protected List<List<Offsets>> offsets;
    public int CLIENT_NUMBERS;
    public int CHUNK_SIZE;

    public final Object lock = new Object();

    public ThreadServer(String filePath){
        try {
            this.randomAccessFile = new RandomAccessFile(filePath, "r");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public void init(Map<String, Object> configMap) {
        this.CHUNK_SIZE = Math.max((int) configMap.getOrDefault("chunkSize", 32), 32);
        this.CLIENT_NUMBERS = Math.min((int) configMap.getOrDefault("clientNumbers", 2), 24);
        this.offsets = this.getOffsets(CLIENT_NUMBERS, CHUNK_SIZE);
        this.alphabetSplit = this.splitAlphabet(CLIENT_NUMBERS);
    }

    public void start() {
        int currentIndex = 0;
        List<String> joinedAlphabetSplit = this.alphabetSplit.stream()
                .map(innerList -> String.join("", innerList))
                .collect(Collectors.toList());
        try {
            while (currentIndex < this.CLIENT_NUMBERS) {
                ThreadClient threadClient = new ThreadClient(this,currentIndex, joinedAlphabetSplit,  offsets.get(currentIndex));
                Thread thread = new Thread(threadClient);
                String key = joinedAlphabetSplit.get(currentIndex);
                this.clientQueue.put(key, threadClient);
                this.clientThreads.put(key, thread);
                currentIndex++;
            }

            clientThreads.values().forEach(Thread::start);

            for (Thread thread : clientThreads.values()) {
                thread.join();
            }

            wordCount.entrySet().stream()
                    .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                    .limit(10)
                    .forEach(entry -> System.out.println(entry.getKey() + ": " + entry.getValue()));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<List<String>> splitAlphabet(int clientNumbers) {
        String alphabet = "abcdefghijklmnopqrstuvwxyz";
        int totalElements = alphabet.length();
        int elementsPerSublist = totalElements / clientNumbers;
        int remainder = totalElements % clientNumbers;

        List<List<String>> outputSublists = new ArrayList<>();
        int index = 0;
        for (int i = 0; i < clientNumbers; i++) {
            List<String> sublist = new ArrayList<>();
            int sublistSize = elementsPerSublist + (i < remainder ? 1 : 0);
            for (int j = 0; j < sublistSize; j++) {
                sublist.add(alphabet.substring(index, index + 1));
                index++;
            }
            outputSublists.add(sublist);
        }
        return outputSublists;
    }

    private int[] getOffsets(int chunkSize) {
        List<Integer> offsets = new ArrayList<>();
        offsets.add(0);

        long fileSizeInBytes;
        try {
            fileSizeInBytes = this.randomAccessFile.length();
            int currentPosition = chunkSize;
            Pattern pattern = Pattern.compile("[a-zA-Z]");
            while (currentPosition < fileSizeInBytes) {
                this.randomAccessFile.seek(currentPosition);
                int ch = this.randomAccessFile.read();
                String character = Character.toString((char) ch);
                Matcher matcher = pattern.matcher(character);
                if (!matcher.matches()) {
                    offsets.add(++currentPosition);
                    currentPosition += chunkSize;
                } else {
                    currentPosition--;
                }
            }
            offsets.add((int) fileSizeInBytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return offsets.stream().mapToInt(Integer::intValue).toArray();
    }

    private List<List<Offsets>> getOffsets(int clientNumbers, int chunkSize) {
        int[] offsets = this.getOffsets(chunkSize);
        int totalElements = offsets.length;
        int elementsPerSublist = totalElements / clientNumbers;
        int remainder = Math.max((totalElements % clientNumbers) - 1, 0);

        List<List<Offsets>> outputOffsetList = new ArrayList<>();
        int index = 0;
        for (int i = 0; i < clientNumbers; i++) {
            List<Offsets> subList = new ArrayList<>();
            int sublistSize = Math.min(elementsPerSublist + (i < remainder ? 1 : 0), totalElements - index - 1);
            for (int j = 0; j < sublistSize; j++) {
                int offset = offsets[index];
                int length = offsets[index + 1] - offset;
                subList.add(new Offsets(offset, length));
                index++;
            }
            outputOffsetList.add(subList);

        }
        return outputOffsetList;
    }

    public byte[] getChunk(int offset, int length) {
        synchronized (this.randomAccessFile) {
            byte[] chunk = new byte[length];
            try {
                this.randomAccessFile.seek(offset);
                this.randomAccessFile.read(chunk, 0, length);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return chunk;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            this.randomAccessFile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
