package org.betriebssysteme.IPCVariants.UDS;

import java.io.*;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.betriebssysteme.Classes.ClientStatus;
import org.betriebssysteme.IPCVariants.TCP.TCPServer;
import org.betriebssysteme.Record.Offsets;
import org.betriebssysteme.Utils.Utils;

public class UnixDomainSocketServer extends TCPServer {
    private List<List<Offsets>> offsets;
    Path socketPath;

    public UnixDomainSocketServer(String filePath) {
        super(filePath);
        this.rapidFlush = true;
    }


    @Override
    public void init(Map<String, Object> configMap) {
        this.CHUNK_SIZE = Math.max((int) configMap.getOrDefault("chunkSize", 32), 32);
        this.CLIENT_NUMBERS = Math.min((int) configMap.getOrDefault("clientNumbers", 2), 24);
        this.offsets = this.getOffsets(CLIENT_NUMBERS, CHUNK_SIZE);
        this.alphabetSplit = this.splitAlphabet(CLIENT_NUMBERS);

        socketPath = FileSystems.getDefault().getPath("./uds/server.socket");
        socketPath.toFile().mkdirs();
        try {
            Files.deleteIfExists(socketPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

    @Override
    public void start() {
        int currentIndex = 0;
        UnixDomainSocketAddress socketAddress = UnixDomainSocketAddress.of(socketPath);
        try(ServerSocketChannel serverChannel = ServerSocketChannel.open(StandardProtocolFamily.UNIX)){
            serverChannel.bind(socketAddress);
            while (currentIndex < this.CLIENT_NUMBERS) {
                SocketChannel channel = serverChannel.accept();
                DataInputStream clientInputStream = new DataInputStream(Channels.newInputStream(channel));
                DataOutputStream clientOutputStream = new DataOutputStream(Channels.newOutputStream(channel));
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