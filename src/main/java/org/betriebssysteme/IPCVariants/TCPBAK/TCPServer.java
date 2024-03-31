package org.betriebssysteme.IPCVariants.TCPBAK;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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

public class TCPServer extends IPCServer {
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
        this.CHUNK_SIZE = (int) configMap.getOrDefault("chunkSize", 32);
        this.CLIENT_NUMBERS = (int) configMap.getOrDefault("clientNumbers", 10);
        this.offsets = this.getOffsets(CLIENT_NUMBERS, CHUNK_SIZE);
        this.alphabetSplit = this.splitAlphabet(CLIENT_NUMBERS);
    }

    public void hotfixInit(OutputStream out, int index) {
        try {
            String result = alphabetSplit.stream()
                    .map(innerList -> String.join("", innerList))
                    .collect(Collectors.joining(EPackage.STRING_DELIMETER));
            result = index + EPackage.STRING_DELIMETER + result;
            byte[] bytes = result.getBytes();
            int offset = 0;
            int chunkSize = TCPMaxPacketSize.PACKET_SIZE;
            int packetSize = bytes.length;
            byte[] subHeader = ByteBuffer.allocate(4).putInt(packetSize).array();

            out.write(EPackage.INIT.getValue());
            out.write(subHeader);

            while (offset < packetSize) {
                chunkSize = Math.min(chunkSize, packetSize - offset);
                out.write(bytes, offset, chunkSize);
                out.flush();
                offset += chunkSize;
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    @Override
    public void start() {
        int currentIndex = 0;
        try (ServerSocket serverSocket = new ServerSocket(this.PORT)) {
            while (currentIndex < this.CLIENT_NUMBERS) {
                Socket clientSocket = serverSocket.accept();
                Thread thread = new Thread(
                        new ClientHandler(this, clientSocket, offsets.get(currentIndex), currentIndex));
                if (currentIndex < alphabetSplit.size()) {
                    String key = alphabetSplit.get(currentIndex).stream().collect(Collectors.joining());
                    this.clientQueue.put(key, clientSocket);
                    this.clientThreads.put(key, thread);
                    this.reduced.put(clientSocket.getOutputStream(), 0);
                } else {
                    this.clientQueue.put(String.valueOf(currentIndex), clientSocket);
                    this.clientThreads.put(String.valueOf(currentIndex), thread);
                }
                this.locks.put(clientSocket.getOutputStream(), 0);
                this.hotfixInit(clientSocket.getOutputStream(), currentIndex);
                currentIndex++;
            }
            for (Thread thread : clientThreads.values()) {
                thread.start();
            }
            for (Thread thread : clientThreads.values()) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            serverSocket.close();
            List<Map.Entry<String, Integer>> entryList = new ArrayList<>(wordCount.entrySet());
            Collections.sort(entryList, Map.Entry.comparingByValue(Comparator.reverseOrder()));

            int count = 0;
            for (Map.Entry<String, Integer> entry : entryList) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
                count++;
                if (count >= 10) {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

class ClientHandler extends Thread implements ISendable<OutputStream> {
    private TCPServer server;
    private Socket socket;
    private InputStream inputStream;
    private OutputStream outputStream;
    private List<Offsets> offsets;
    private int clientIndex = 0;
    private EClientStatus eClientStatus = EClientStatus.WORKING;
    private int currentOffset = 0;
    private EPackage lastHeader;

    public ClientHandler(TCPServer server, Socket socket, List<Offsets> offsets, int clientIndex) {
        this.server = server;
        this.socket = socket;
        this.offsets = offsets;
        this.clientIndex = clientIndex;
    }

    private byte[] readPackage(InputStream inputStream) throws IOException {
        int offset = 0;
        int chunkSize = TCPMaxPacketSize.PACKET_SIZE;
        byte[] subHeader = new byte[4];
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
                    lastHeader = EPackage.fromByte(header);
                    if (header == -1) {
                        break;
                    }
                    switch (EPackage.fromByte(header)) {
                        case CONNECTED:
                        case MAP:
                            if (currentOffset < offsets.size()) {
                                Offsets offset = offsets.get(currentOffset);
                                byte[] packet = this.server.getChunk(offset.offset(), offset.length());
                                send(this.outputStream, EPackage.MAP, packet);
                                eClientStatus = EClientStatus.WORKING;
                                currentOffset++;
                            } else {
                                send(this.outputStream, EPackage.SHUFFLE, null);
                            }
                            break;
                        case SHUFFLE:
                            data = this.readPackage(inputStream);
                            message = new String(data, StandardCharsets.UTF_8);
                            parts = message.split("\\" + EPackage.STRING_DELIMETER);
                            i = 0;
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
                                        OutputStream out = server.clientQueue.get(key).getOutputStream();
                                        data = sj.toString().getBytes(StandardCharsets.UTF_8);
                                        send(out, EPackage.REDUCE, data);
                                    } else {
                                        // FALLS EIN CLIENT ABSCHMIERT
                                        System.out.println("SERVER INSERT" + sj.toString());
                                    }
                                }
                            }
                            if (this.clientIndex > server.alphabetSplit.size() - 1) {
                                this.send(this.outputStream, EPackage.MERGE, null);
                            }
                            break;
                        case MERGE:
                            data = this.readPackage(inputStream);
                            message = new String(data, StandardCharsets.UTF_8);
                            parts = message.split("\\" + EPackage.STRING_DELIMETER);
                            for (i = 0; i < parts.length-1; i = i + 2) {
                                String word = parts[i];
                                try {
                                    int count = Integer.parseInt(parts[i + 1]);
                                    if (server.wordCount.containsKey(word)) {
                                        server.wordCount.put(word, server.wordCount.get(word) + count);
                                    } else {
                                        server.wordCount.put(word, count);
                                    }
                                } catch (Exception e) {
                                    System.out.println("MERGE ERROR: " + e.getMessage());
                                }
                                int count = Integer.parseInt(parts[i + 1]);
                                if (server.wordCount.containsKey(word)) {
                                    server.wordCount.put(word, server.wordCount.get(word) + count);
                                } else {
                                    server.wordCount.put(word, count);
                                }

                            }
                            send(this.outputStream, EPackage.DONE, null);
                            this.eClientStatus = EClientStatus.DONE;
                        default:
                            // Handle default case
                            break;
                    }
                    System.out.println("LAST" + this.clientIndex + " : " + lastHeader);
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

    @Override
    public void send(OutputStream out, EPackage header, byte[] bytes) {
        try {
            synchronized (this.server.locks.get(out)) {
                switch (header) {
                    case MAP:
                        int offset = 0;
                        int chunkSize = TCPMaxPacketSize.PACKET_SIZE;
                        int packetSize = bytes.length;
                        byte[] subHeader = ByteBuffer.allocate(4).putInt(packetSize).array();

                        out.write(header.getValue());
                        out.write(subHeader);

                        while (offset < packetSize) {
                            chunkSize = Math.min(chunkSize, packetSize - offset);
                            out.write(bytes, offset, chunkSize);
                            //out.flush();
                            offset += chunkSize;
                        }
                        break;
                    case REDUCE:
                        offset = 0;
                        chunkSize = TCPMaxPacketSize.PACKET_SIZE;
                        packetSize = bytes.length;
                        subHeader = ByteBuffer.allocate(4).putInt(packetSize).array();

                        out.write(EPackage.REDUCE.getValue());
                        out.write(subHeader);

                        while (offset < packetSize) {
                            chunkSize = Math.min(chunkSize, packetSize - offset);
                            out.write(bytes, offset, chunkSize);
                            //out.flush();
                            offset += chunkSize;
                        }

                        int r = this.server.reduced.get(out);
                        this.server.reduced.put(out, r + 1);
                        if (this.server.reduced.get(out) >= server.CLIENT_NUMBERS - 1) {
                            out.write(EPackage.MERGE.getValue());
                            //out.flush();
                        }
                        break;
                    case SHUFFLE:
                    case MERGE:
                    case CONNECTED:
                    case DONE:
                        out.write(header.getValue());
                        //out.flush();
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