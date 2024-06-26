package org.betriebssysteme.IPCVariants.PIPE;

import org.betriebssysteme.Classes.ClientStatus;
import org.betriebssysteme.Classes.OutputStreamClientHandler;
import org.betriebssysteme.Classes.OutputStreamServer;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PipeServer extends OutputStreamServer {
    private List<Process> processList = new ArrayList<>();


    public PipeServer(String filePath) {
        super(filePath);
        this.rapidFlush = true;
    }

    @Override
    public void init(Map<String, Object> configMap) {
        this.CHUNK_SIZE = Math.max((int) configMap.getOrDefault("chunkSize", 32), 32);
        this.CLIENT_NUMBERS = Math.min((int) configMap.getOrDefault("clientNumbers", 2), 24);
        this.offsets = this.getOffsets(CLIENT_NUMBERS, CHUNK_SIZE);
        this.alphabetSplit = this.splitAlphabet(CLIENT_NUMBERS);
    }

    public void setProcessList(List<Process> processList) {
        this.processList = processList;
    }

    @Override
    public void start() {
        int currentIndex = 0;
        for (Process process : processList) {
            DataInputStream clientInputStream = new DataInputStream(process.getInputStream());
            DataOutputStream clientOutputStream = new DataOutputStream(process.getOutputStream());
            Thread thread = new Thread(
                    new OutputStreamClientHandler(this, clientInputStream, clientOutputStream, offsets.get(currentIndex)));
            String key = String.join("", alphabetSplit.get(currentIndex));
            this.clientQueue.put(key, clientOutputStream);
            this.clientThreads.put(key, thread);
            this.clientStatus.put(clientOutputStream, new ClientStatus());
            this.generateInitMessage(clientOutputStream, currentIndex);
            currentIndex++;
        }


        clientThreads.values().forEach(Thread::start);

        try {
            for (Thread thread : clientThreads.values()) {
                thread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        wordCount.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(10)
                .forEach(entry -> System.out.println(entry.getKey() + ": " + entry.getValue()));

    }
}