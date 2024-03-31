package org.betriebssysteme.IPCVariants.TCP;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
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

public class TCPServer extends IPCServer implements ISendable<OutputStream> {
    ConcurrentHashMap<String, Socket> clientQueue = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, Thread> clientThreads = new ConcurrentHashMap<>();
    List<List<String>> alphabetSplit = new ArrayList<>();
    ConcurrentHashMap<OutputStream, Integer> reduced = new ConcurrentHashMap<>();
    ConcurrentHashMap<OutputStream, Object> locks = new ConcurrentHashMap<>();
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
                Thread thread = new Thread(
                        new ClientHandler(this, clientSocket, offsets.get(currentIndex)));
                String key = alphabetSplit.get(currentIndex).stream().collect(Collectors.joining());
                this.clientQueue.put(key, clientSocket);
                this.clientThreads.put(key, thread);
                this.reduced.put(clientSocket.getOutputStream(), 0);
                this.locks.put(clientSocket.getOutputStream(), new Object());
                this.generateInitMessage(clientSocket.getOutputStream(), currentIndex);
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

    private void generateInitMessage(OutputStream out, int index) throws IOException {
        String result = index + EPackage.STRING_DELIMETER +
                alphabetSplit.stream()
                        .map(innerList -> String.join("", innerList))
                        .collect(Collectors.joining(EPackage.STRING_DELIMETER));
        this.send(out, EPackage.INIT, result.getBytes());

    }

    @Override
    public void send(OutputStream out, EPackage header, byte[] bytes) {
        try {
            synchronized (this.locks.get(out)) {
                int offset = 0;
                int chunkSize = TCPMaxPacketSize.PACKET_SIZE;
                int packetSize = (bytes != null) ? bytes.length : 0;
                byte[] subHeader = ByteBuffer.allocate(EPackage.PACKET_SIZE_LENGTH).putInt(packetSize).array();

                switch (header) {
                    case INIT:
                    case MAP:
                    case REDUCE:
                        out.write(header.getValue());
                        out.write(subHeader);
                        while (offset < packetSize) {
                            chunkSize = Math.min(chunkSize, packetSize - offset);
                            out.write(bytes, offset, chunkSize);
                            offset += chunkSize;
                        }
                        if (header == EPackage.INIT) {
                            out.flush();
                        }
                        if (header == EPackage.REDUCE
                                && this.reduced.merge(out, 1, Integer::sum) >= this.CLIENT_NUMBERS - 1) {
                            out.write(EPackage.MERGE.getValue());
                        }
                        break;
                    case SHUFFLE:
                    case MERGE:
                    case CONNECTED:
                    case DONE:
                        out.write(header.getValue());
                        break;
                    default:
                        break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

class ClientHandler extends Thread {
    private TCPServer server;
    private Socket socket;
    private InputStream inputStream;
    private OutputStream outputStream;
    private List<Offsets> offsets;
    private EClientStatus eClientStatus = EClientStatus.WORKING;
    private int currentOffset = 0;
    private EPackage lastHeader;

    public ClientHandler(TCPServer server, Socket socket, List<Offsets> offsets) {
        this.server = server;
        this.socket = socket;
        this.offsets = offsets;
    }

    private byte[] readPackage(InputStream inputStream) throws IOException {
        int offset = 0;
        int chunkSize = TCPMaxPacketSize.PACKET_SIZE;
        byte[] subHeader = new byte[EPackage.PACKET_SIZE_LENGTH];
        inputStream.read(subHeader);
        int packetSize = ByteBuffer.wrap(subHeader).getInt();
        byte[] data = new byte[packetSize];

        while (offset < packetSize) {
            chunkSize = Math.min(chunkSize, packetSize - offset);
            inputStream.read(data, offset, chunkSize);
            offset += chunkSize;
        }
        return data;
    }

    // RECEIVING from client
    public void run() {
        byte[] data;
        String message;
        String[] parts;
        int i;
        try {
            this.inputStream = socket.getInputStream();
            this.outputStream = socket.getOutputStream();
            synchronized (this.inputStream) {
                while (eClientStatus != EClientStatus.DONE) {
                    byte header = (byte) inputStream.read();
                    if (header == -1) {
                        break;
                    }
                    lastHeader = EPackage.fromByte(header);
                    switch (EPackage.fromByte(header)) {
                        case CONNECTED:
                        case MAP:
                            if (currentOffset < offsets.size()) {
                                Offsets offset = offsets.get(currentOffset);
                                byte[] packet = this.server.getChunk(offset.offset(), offset.length());
                                this.server.send(this.outputStream, EPackage.MAP, packet);
                                currentOffset++;
                            } else {
                                this.server.send(this.outputStream, EPackage.SHUFFLE, null);
                            }
                            break;
                        case SHUFFLE:
                            data = this.readPackage(inputStream);
                            System.out.println("Received Shuffle " + data.length);
                            message = new String(data);
                            parts = message.split(EPackage.STRING_DELIMETER);
                            i = 0;
                            if (parts.length > 1) {
                                while (i < parts.length) {
                                    String key = parts[i];
                                    StringJoiner sj = new StringJoiner(EPackage.STRING_DELIMETER);
                                    int count = Integer.parseInt(parts[i + 1]) * 2;
                                    i = i + 2;
                                    count = count + i;
                                    while (i < count) {
                                        if(i+1 >= parts.length){
//                                            System.out.println(message);
                                            System.out.println("what?");
                                        }
                                        sj.add(parts[i]);
                                        sj.add(parts[i + 1]);
                                        i = i + 2;
                                    }
                                    if (server.clientQueue.containsKey(key)) {
                                        OutputStream out = server.clientQueue.get(key).getOutputStream();
                                        data = sj.toString().getBytes();
                                        this.server.send(out, EPackage.REDUCE, data);
                                        System.out.println("Send Reduced " + data.length);
                                    }
                                }
                            }
                            break;
                        case MERGE:
                            data = this.readPackage(inputStream);
                            message = new String(data);
                            parts = message.split(EPackage.STRING_DELIMETER);
                            for (i = 0; i < parts.length - 1; i = i + 2) {
                                String word = parts[i];
                                int count = Integer.parseInt(parts[i + 1]);
                                synchronized(server.wordCount){
                                    server.wordCount.compute(word, (k, v) -> (v == null) ? count : v + count);
                                }
                            }
                            this.server.send(this.outputStream, EPackage.DONE, null);
                            this.eClientStatus = EClientStatus.DONE;
                        default:
                            break;
                    }
                }
            }
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