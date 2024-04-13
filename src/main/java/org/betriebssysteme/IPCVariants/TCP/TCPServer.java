package org.betriebssysteme.IPCVariants.TCP;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import org.betriebssysteme.Classes.ClientStatus;
import org.betriebssysteme.Classes.OutputStreamClientHandler;
import org.betriebssysteme.Classes.OutputStreamServer;
import org.betriebssysteme.Record.Offsets;

public class TCPServer extends OutputStreamServer {
    private int PORT;

    public TCPServer(String filePath) {
        super(filePath);

    }

    @Override
    public void init(Map<String, Object> configMap) {
        this.PORT = (int) configMap.getOrDefault("port", 42069);
        this.CHUNK_SIZE = Math.max((int) configMap.getOrDefault("chunkSize", 32), 32);
        this.CLIENT_NUMBERS = Math.min((int) configMap.getOrDefault("clientNumbers", 2), 24);
        this.offsets = this.getOffsets(CLIENT_NUMBERS, CHUNK_SIZE);
        this.alphabetSplit = this.splitAlphabet(CLIENT_NUMBERS);
    }

    @Override
    public void start() {
        int currentIndex = 0;
        try (ServerSocket serverSocket = new ServerSocket(this.PORT)) {
            while (currentIndex < this.CLIENT_NUMBERS) {
                Socket clientSocket = serverSocket.accept();
                DataInputStream clientInputStream = new DataInputStream(clientSocket.getInputStream());
                DataOutputStream clientOutputStream = new DataOutputStream(clientSocket.getOutputStream());
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

            for (Thread thread : clientThreads.values()) {
                thread.join();
            }

            serverSocket.close();

            wordCount.entrySet().stream()
                    .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                    .limit(10)
                    .forEach(entry -> System.out.println(entry.getKey() + ": " + entry.getValue()));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }




}
