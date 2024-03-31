package org.betriebssysteme.IPCVariants.TCP;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.betriebssysteme.Classes.IPCServer;
import org.betriebssysteme.Enum.EClientStatus;
import org.betriebssysteme.Enum.EPackage;
import org.betriebssysteme.Interfaces.ISendable;
import org.betriebssysteme.Record.Offsets;

public class TCPServer extends IPCServer implements ISendable<DataOutputStream> {
    ConcurrentHashMap<String, DataOutputStream> clientQueue = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, Thread> clientThreads = new ConcurrentHashMap<>();
    List<List<String>> alphabetSplit = new ArrayList<>();
    ConcurrentHashMap<OutputStream, Integer> reduced = new ConcurrentHashMap<>();
    HashMap<String, Integer> wordCount = new HashMap<>();

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
                String key = alphabetSplit.get(currentIndex).stream().collect(Collectors.joining());
                this.clientQueue.put(key, clientOutputStream);
                this.clientThreads.put(key, thread);
                this.reduced.put(clientOutputStream, 0);
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

    private void generateInitMessage(DataOutputStream out, int index) throws IOException {
        String result = index + EPackage.STRING_DELIMETER +
                alphabetSplit.stream()
                        .map(innerList -> String.join("", innerList))
                        .collect(Collectors.joining(EPackage.STRING_DELIMETER));
        this.send(out, EPackage.INIT, result);

    }

    @Override
    public void send(DataOutputStream out, EPackage header, String message) {
        try {
            // synchronized (out) {
            switch (header) {
                case INIT:
                case MAP:
                case REDUCE:
                    out.writeByte(header.getValue());
                    byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
                    out.writeInt(bytes.length);
                    out.write(bytes);
                    if (header == EPackage.INIT) {
                        out.flush();
                    }
                    if (header == EPackage.REDUCE
                            && this.reduced.merge(out, 1, Integer::sum) >= this.CLIENT_NUMBERS - 1) {
                        out.writeByte(EPackage.MERGE.getValue());
                    }
                    break;
                case SHUFFLE:
                case MERGE:
                case CONNECTED:
                case DONE:
                    out.writeByte(header.getValue());
                    break;
                default:
                    break;
            }
            // }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

class ClientHandler extends Thread {
    private TCPServer server;
    private Socket socket;
    private DataInputStream inputStream;
    private DataOutputStream outputStream;
    private List<Offsets> offsets;
    private EClientStatus eClientStatus = EClientStatus.WORKING;
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
        int dataSize;
        byte[] data;
        String message;

        try {
            // synchronized (this.server) {
            while (eClientStatus != EClientStatus.DONE) {
                byte header = inputStream.readByte();
                if (header == -1) {
                    break;
                }

                switch (EPackage.fromByte(header)) {
                    case CONNECTED:
                    case MAP:
                        if (currentOffset < offsets.size()) {
                            Offsets offset = offsets.get(currentOffset);
                            byte[] packet = this.server.getChunk(offset.offset(), offset.length());
                            message = new String(packet);
                            this.server.send(this.outputStream, EPackage.MAP, message);
                            currentOffset++;
                        } else {
                            this.server.send(this.outputStream, EPackage.SHUFFLE, null);
                        }
                        break;
                    case SHUFFLE:
                        dataSize = this.inputStream.readInt();
                        data = new byte[dataSize];
                        this.inputStream.readFully(data);
                        message = new String(data, StandardCharsets.UTF_8);
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
                                    if (i + 1 >= parts.length) {
                                        // System.out.println("Received Shuffle " + data.length);
                                        // System.out.println(message);
                                    }
                                    sj.add(parts[i]);
                                    sj.add(parts[i + 1]);
                                    i = i + 2;
                                }
                                if (server.clientQueue.containsKey(key)) {
                                    DataOutputStream out = server.clientQueue.get(key);
                                    this.server.send(out, EPackage.REDUCE, sj.toString());
                                    // System.out.println("Send Reduced " + data.length);
                                }
                            }
                        }
                        if (currentOffset >= offsets.size() && (this.server.reduced.get(this.outputStream)  >= this.server.CLIENT_NUMBERS - 1)) {
                            this.server.send(this.outputStream, EPackage.MERGE, null);
                        }
                        break;
                    case MERGE:
                        dataSize = this.inputStream.readInt();
                        data = new byte[dataSize];
                        this.inputStream.readFully(data);
                        message = new String(data, StandardCharsets.UTF_8);
                        parts = message.split(EPackage.STRING_DELIMETER);
                        for (i = 0; i < parts.length - 1; i = i + 2) {
                            String word = parts[i];
                            int count = Integer.parseInt(parts[i + 1]);
                            synchronized (server.wordCount) {
                                server.wordCount.compute(word, (k, v) -> (v == null) ? count : v + count);
                            }
                        }
                        if (currentOffset >= offsets.size() && (this.server.reduced.get(this.outputStream)  >= this.server.CLIENT_NUMBERS - 1)) {
                            this.server.send(this.outputStream, EPackage.DONE, null);
                            this.eClientStatus = EClientStatus.DONE;
                        }
                        break;
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

    public void close() {
        try {
            this.socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}