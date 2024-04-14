package org.betriebssysteme.Plain.Threaded;

import org.betriebssysteme.Record.Offsets;
import org.betriebssysteme.Utils.Utils;

import java.util.HashMap;
import java.util.List;

public class ThreadClient implements Runnable {
    private final ThreadServer threadServer;
    private final int clientIndex;
    private final List<String> alphabetSplit;
    private final List<Offsets> offsets;

    private final HashMap<String, HashMap<String, Integer>> wordCount = new HashMap<>();

    private int currentOffset = 0;
    private int reduced = 0;
    private final Object lock = new Object(); // Lock object for synchronization


    ThreadClient(ThreadServer threadServer, int clientIndex, List<String> alphabetSplit, List<Offsets> offsets) {
        this.threadServer = threadServer;
        this.clientIndex = clientIndex;
        this.alphabetSplit = alphabetSplit;
        this.offsets = offsets;

        for (String s : alphabetSplit) {
            Utils.associateKeys(this.wordCount, s, new HashMap<>());
        }


    }

    @Override
    public void run() {
        while (currentOffset < offsets.size()) {
            Offsets offset = offsets.get(currentOffset);
            byte[] packet = this.threadServer.getChunk(offset.offset(), offset.length());
            String message = new String(packet);
            synchronized (this.wordCount) {
                Utils.countWords(this.wordCount, message);
            }
            currentOffset++;
        }

        for (int i = 0; i < this.alphabetSplit.size(); i++) {
            if (this.clientIndex == i)
                continue;
            ThreadClient threadClient = this.threadServer.clientQueue.get(this.alphabetSplit.get(i));
            threadClient.reduce(this.wordCount.get(this.alphabetSplit.get(i)));
            this.wordCount.get(this.alphabetSplit.get(i)).clear();
        }


        synchronized (this.threadServer.lock) {
            while ((reduced < this.threadServer.CLIENT_NUMBERS - 1) && (this.threadServer.CLIENT_NUMBERS != 1)) {
                try {
                    this.threadServer.lock.wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        wordCount.get(this.alphabetSplit.get(this.clientIndex))
                .forEach((key, value) -> this.threadServer.wordCount
                        .merge(key, value, Integer::sum));

    }

    public void reduce(HashMap<String, Integer> shuffledWordCount) {
        if (shuffledWordCount != null) {
            synchronized (this.wordCount) {
                shuffledWordCount
                        .forEach((key, value) -> wordCount.get(this.alphabetSplit.get(this.clientIndex))
                                .merge(key, value, Integer::sum));
            }
        }
        this.reduced++;
        synchronized (this.threadServer.lock) {
            this.threadServer.lock.notifyAll();
        }

    }
}