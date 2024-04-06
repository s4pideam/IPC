package org.betriebssysteme.IPCVariants.TCP;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.betriebssysteme.Classes.ClientStatus;
import org.betriebssysteme.Classes.IPCServer;
import org.betriebssysteme.Enum.EPackage;
import org.betriebssysteme.Interfaces.ISendable;
import org.betriebssysteme.Record.Offsets;

public class TCPServer extends IPCServer implements ISendable<DataOutputStream> {
    ConcurrentHashMap<String, DataOutputStream> clientQueue = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, Thread> clientThreads = new ConcurrentHashMap<>();
    List<List<String>> alphabetSplit = new ArrayList<>();
    ConcurrentHashMap<OutputStream, ClientStatus> clientStatus = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, Integer> wordCount = new ConcurrentHashMap<>();

    private int PORT;
    private int CHUNK_SIZE;
    public int CLIENT_NUMBERS;

    private List<List<Offsets>> offsets;

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
                        new ClientHandler(this, clientInputStream, clientOutputStream, offsets.get(currentIndex)));
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

    private void generateInitMessage(DataOutputStream out, int index) {
        String result = index + EPackage.STRING_DELIMETER +
                alphabetSplit.stream()
                        .map(innerList -> String.join("", innerList))
                        .collect(Collectors.joining(EPackage.STRING_DELIMETER));
        this.send(out, EPackage.INIT, result);

    }

    @Override
    public void send(DataOutputStream out, EPackage header, String message) {
        try {
            synchronized (out) {
                switch (header) {
                    case INIT:
                    case MAP:
                    case REDUCE: {
                        out.writeByte(header.getValue());
                        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
                        out.writeInt(bytes.length);
                        out.write(bytes);
                        if (header == EPackage.REDUCE) {
                            synchronized (this) {
                                ClientStatus clientStatus = this.clientStatus.get(out);
                                clientStatus.REDUCED++;
                                if ((clientStatus.REDUCED == this.CLIENT_NUMBERS - 1) && (clientStatus.MAPPED)
                                        && (!clientStatus.MERGED)) {
                                    out.writeByte(EPackage.MERGE.getValue());
                                }
                            }
                        }
                        if (header == EPackage.INIT) {
                            out.flush();
                        }
                        break;
                    }
                    case SHUFFLE:
                    case MERGE:
                    case CONNECTED:
                    case DONE: {
                        out.writeByte(header.getValue());
                        break;
                    }
                    default:
                        break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Eigentlich unnötig hier zu synchronizieren. Und der zweite Fall tritt niemals ein (v+count)
    public synchronized void updateWordCount(String word, int count) {
        wordCount.compute(word, (k, v) -> (v == null) ? count : v + count);
    }

}

class ClientHandler extends Thread {
    private final TCPServer server;
    private final DataInputStream inputStream;
    private final DataOutputStream outputStream;
    private final List<Offsets> offsets;
    private int currentOffset = 0;

    public ClientHandler(TCPServer server, DataInputStream dataInputStream, DataOutputStream dataOutputStream,
            List<Offsets> offsets) {
        this.server = server;
        this.inputStream = dataInputStream;
        this.outputStream = dataOutputStream;
        this.offsets = offsets;
    }

    // RECEIVING from client
    public void run() {

        try {
            // synchronized (this.server) {
            while (!this.server.clientStatus.get(this.outputStream).FINISHED) {
                byte header = inputStream.readByte();
                if (header == -1) {
                    break;
                }

                switch (EPackage.fromByte(header)) {
                    case CONNECTED:
                    case MAP: {
                        if (currentOffset < offsets.size()) {
                            Offsets offset = offsets.get(currentOffset);
                            byte[] packet = this.server.getChunk(offset.offset(), offset.length());
                            String message = new String(packet);
                            this.server.send(this.outputStream, EPackage.MAP, message);
                            currentOffset++;
                        } else {
                            this.server.clientStatus.get(this.outputStream).MAPPED = true;
                            this.server.send(this.outputStream, EPackage.SHUFFLE, null);
                        }
                        break;
                    }
                    case SHUFFLE: {
                        int dataSize = this.inputStream.readInt();
                        byte[] data = new byte[dataSize];
                        this.inputStream.readFully(data);
                        String message = new String(data, StandardCharsets.UTF_8);
                        String[] parts = message.split(EPackage.STRING_DELIMETER);
                        int i = 0;
                        if (parts.length > 1) {
                            while (i < parts.length) {
                                String key = parts[i];
                                StringJoiner sj = new StringJoiner(EPackage.STRING_DELIMETER);
                                int count = Integer.parseInt(parts[i + 1]) * 2;
                                i = i + 2;
                                count = count + i;
                                while (i < count) {
                                    sj.add(parts[i]);
                                    sj.add(parts[i + 1]);
                                    i = i + 2;
                                }
                                if (server.clientQueue.containsKey(key)) {
                                    DataOutputStream out = server.clientQueue.get(key);
                                    this.server.send(out, EPackage.REDUCE, sj.toString());
                                }
                            }
                        }

                        ClientStatus clientstatus = this.server.clientStatus.get(this.outputStream);
                        clientstatus.SHUFFLED = true;

                        if ((this.server.CLIENT_NUMBERS == 1)
                                || (clientstatus.MAPPED
                                        && clientstatus.REDUCED == this.server.CLIENT_NUMBERS - 1)) {
                            this.server.send(this.outputStream, EPackage.MERGE, null);
                        }
                        break;
                    }
                    case MERGE:{
                        int dataSize = this.inputStream.readInt();
                        byte[] data = new byte[dataSize];
                        this.inputStream.readFully(data);
                        String message = new String(data, StandardCharsets.UTF_8);
                        String[] parts = message.split(EPackage.STRING_DELIMETER);
                        for (int i = 0; i < parts.length - 1; i = i + 2) {
                            String word = parts[i];
                            int count = Integer.parseInt(parts[i + 1]);
                            this.server.updateWordCount(word, count);
                        }
                        this.server.clientStatus.get(this.outputStream).MERGED = true;
                        this.server.send(this.outputStream, EPackage.DONE, null);
                        this.server.clientStatus.get(this.outputStream).FINISHED = true;
                        break;
                    }
                    default:
                        break;
                }
            }
            // }
        } catch (Exception e) {

            System.out.println("Server exception: " + e.getMessage());
            e.printStackTrace();
        }
    }

}