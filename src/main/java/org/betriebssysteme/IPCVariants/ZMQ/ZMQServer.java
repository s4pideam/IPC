package org.betriebssysteme.IPCVariants.ZMQ;

import org.betriebssysteme.Classes.ClientHandler;
import org.betriebssysteme.Classes.ClientStatus;
import org.betriebssysteme.IPCVariants.TCP.TCPServer;
import org.betriebssysteme.Record.Offsets;
import org.betriebssysteme.Utils.Utils;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class ZMQServer extends TCPServer {
    private List<List<Offsets>> offsets;

    public ZMQServer(String filePath) {
        super(filePath);

    }

    private Path[] serverToClientNamedPipes;
    private Path[] clientToServerNamedPipes;

    @Override
    public void init(Map<String, Object> configMap) {
        this.CHUNK_SIZE = Math.max((int) configMap.getOrDefault("chunkSize", 32), 32);
        this.CLIENT_NUMBERS = Math.min((int) configMap.getOrDefault("clientNumbers", 2), 24);
        this.offsets = this.getOffsets(CLIENT_NUMBERS, CHUNK_SIZE);
        this.alphabetSplit = this.splitAlphabet(CLIENT_NUMBERS);

        serverToClientNamedPipes = new Path[CLIENT_NUMBERS];
        clientToServerNamedPipes = new Path[CLIENT_NUMBERS];

        for (int i = 0; i < this.CLIENT_NUMBERS; i++) {
            // init named pipes
            Path serverToClient = FileSystems.getDefault().getPath("./np/server_to_client" + i);
            serverToClientNamedPipes[i] = serverToClient;
            Path clientToServer = FileSystems.getDefault().getPath("./np/client_to_server" + i);
            clientToServerNamedPipes[i] = clientToServer;
            Utils.createNamedPipe(serverToClient);
            Utils.createNamedPipe(clientToServer);
        }
    }

    @Override
    public void start() {
        int currentIndex = 0;
        try {
            while (currentIndex < this.CLIENT_NUMBERS) {
                DataInputStream clientInputStream = new DataInputStream(
                        new FileInputStream(clientToServerNamedPipes[currentIndex].toString()));
                DataOutputStream clientOutputStream = new DataOutputStream(
                        new FileOutputStream(serverToClientNamedPipes[currentIndex].toString()));
                Thread thread = new Thread(
                        new ClientHandler(this, clientInputStream, clientOutputStream, offsets.get(currentIndex)));
                String key = String.join("", alphabetSplit.get(currentIndex));
                this.clientQueue.put(key, clientOutputStream);
                this.clientThreads.put(key, thread);
                this.clientStatus.put(clientOutputStream, new ClientStatus());
                this.generateInitMessage(clientOutputStream, currentIndex);
                currentIndex++;
            }
        } catch (IOException e) {
            System.err.println(e);
        }
        ;

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