package org.betriebssysteme.IPCVariants.TCPBAK;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import org.betriebssysteme.Enum.EClientStatus;
import org.betriebssysteme.Enum.EPackage;
import org.betriebssysteme.Interfaces.IIPCClient;
import org.betriebssysteme.Interfaces.ISendable;

public class TCPClient implements IIPCClient, ISendable<OutputStream> {
    private Socket socket;
    private OutputStream outputStream;
    private InputStream inputStream;
    private EClientStatus eClientStatus = EClientStatus.WORKING;
    private int PORT;
    private String HOST;

    private HashMap<String, HashMap<String, Integer>> wordCount = new HashMap<>();
    private int clientIndex = 0;
    private String[] alphabetSplit;

    private EPackage lastHeader;

    private void associateKeys(String keys, HashMap<String, HashMap<String, Integer>> hashMap,
            HashMap<String, Integer> object) {
        hashMap.put(keys, object);
        for (char key : keys.toCharArray()) {
            hashMap.put(String.valueOf(key), object);
        }
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

    private void countWorkds(String message) {
        String[] words = message.toLowerCase().split("\\W+");

        for (String word : words) {
            if (!word.matches("[a-z]+"))
                continue;
            String key = String.valueOf(word.charAt(0));

            if (wordCount.get(key).containsKey(word)) {
                wordCount.get(key).put(word, wordCount.get(key).get(word) + 1);
            } else {
                wordCount.get(key).put(word, 1);
            }

        }
    }

    // SERVER RECEIVE
    @Override
    public void connect() {
        try {
            this.socket = new Socket(this.HOST, this.PORT);
            this.outputStream = socket.getOutputStream();
            this.inputStream = socket.getInputStream();

            while (eClientStatus != EClientStatus.DONE) {
                byte header = (byte) inputStream.read();
                if (header == -1) {
                    break;
                }
                lastHeader = EPackage.fromByte(header);
                switch (EPackage.fromByte(header)) {
                    case INIT:
                        byte[] data = this.readPackage(inputStream);
                        String message = new String(data, StandardCharsets.UTF_8);
                        String[] parts = message.split("\\|");
                        this.clientIndex = Integer.parseInt(parts[0]);
                        this.alphabetSplit = new String[parts.length - 1];
                        for (int i = 1; i < parts.length; i++) {
                            this.alphabetSplit[i - 1] = parts[i];
                            this.associateKeys(parts[i], this.wordCount, new HashMap<>());
                        }
                        this.send(this.outputStream, EPackage.CONNECTED, null);
                        break;
                    case MAP:
                        data = this.readPackage(inputStream);
                        message = new String(data, StandardCharsets.UTF_8);
                        this.countWorkds(message);
                        this.send(this.outputStream, EPackage.MAP, null);
                        break;
                    case SHUFFLE:
                        StringJoiner sj = new StringJoiner(EPackage.STRING_DELIMETER);
                        for (int i = 0; i < this.alphabetSplit.length; i++) {
                            if (this.clientIndex == i)
                                continue;
                            sj.add(this.alphabetSplit[i]);
                            sj.add(String.valueOf(this.wordCount.get(this.alphabetSplit[i]).size()));
                            for (String key : this.wordCount.get(this.alphabetSplit[i]).keySet()) {
                                sj.add(key);
                                sj.add(String.valueOf(this.wordCount.get(this.alphabetSplit[i]).get(key)));
                            }
                            // this.wordCount.get(this.alphabetSplit[i]).clear();
                        }
                        this.send(this.outputStream, EPackage.SHUFFLE, sj.toString().getBytes(StandardCharsets.UTF_8));
                        break;
                    case REDUCE:
                        data = this.readPackage(inputStream);
                        eClientStatus = EClientStatus.WORKING;
                        message = new String(data, StandardCharsets.UTF_8);
                        parts = message.split("\\" + EPackage.STRING_DELIMETER);
                        if (parts.length > 1) {
                            for (int i = 0; i < parts.length; i += 2) {
                                String word = parts[i];
                                int count = Integer.parseInt(parts[i + 1]);
                                String key = String.valueOf(word.charAt(0));
                                if (wordCount.get(key) == null) {
                                    System.out.println("Key not found: " + word);
                                }
                                if (wordCount.get(key).containsKey(word)) {
                                    wordCount.get(key).put(word, wordCount.get(key).get(word) + count);
                                } else {
                                    wordCount.get(key).put(word, count);
                                }
                            }
                        }
                        break;
                    case MERGE:
                        sj = new StringJoiner(EPackage.STRING_DELIMETER);
                        for (Map.Entry<String, Integer> entry : this.wordCount.get(this.alphabetSplit[this.clientIndex])
                                .entrySet()) {
                            sj.add(entry.getKey());
                            sj.add(String.valueOf(entry.getValue()));
                        }
                        data = sj.toString().getBytes(StandardCharsets.UTF_8);
                        send(this.outputStream, EPackage.MERGE, data);
                        break;
                    case DONE:
                        eClientStatus = EClientStatus.DONE;
                        this.disconnect();
                        break;
                    default:
                        // Handle default case
                        break;
                }
                System.out.println("CLIENTLAST" + this.clientIndex + " : " + lastHeader);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void disconnect() {
        try {
            this.socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void init(Map<String, Object> configMap) {
        this.PORT = (int) configMap.getOrDefault("port", 42069);
        this.HOST = (String) configMap.getOrDefault("host", "localhost");
    }

    @Override
    public void send(OutputStream out, EPackage header, byte[] bytes) {
        try {
            synchronized (out) {
                switch (header) {
                    case MERGE:
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
                    case SHUFFLE:
                         offset = 0;
                         chunkSize = TCPMaxPacketSize.PACKET_SIZE;
                         packetSize = bytes.length;
                        subHeader = ByteBuffer.allocate(4).putInt(packetSize).array();

                        out.write(header.getValue());
                        out.write(subHeader);
                        while (offset < packetSize) {
                            chunkSize = Math.min(chunkSize, packetSize - offset);
                            out.write(bytes, offset, chunkSize);
                            //out.flush();
                            offset += chunkSize;
                        }
                        break;
                    case INIT:
                    case CONNECTED:
                    case MAP:
                        out.write(header.getValue());
                        //out.flush();
                        break;
                    default:
                        // Handle default case
                        break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
