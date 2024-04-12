package org.betriebssysteme.IPCVariants.UDS;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import org.betriebssysteme.Classes.IPCClient;
import org.betriebssysteme.Enum.EPackage;
import org.betriebssysteme.Utils.Utils;

public class UDSClient extends IPCClient {
    private SocketChannel channel;

    private final Path SOCKET_FILE_PATH = Path.of("/tmp/uds_server.sock");


    private final HashMap<String, HashMap<String, Integer>> wordCount = new HashMap<>();
    private int clientIndex = 0;
    private String[] alphabetSplit;

    @Override
    public void init(Map<String, Object> configMap) {
        try {
            this.channel = SocketChannel.open(StandardProtocolFamily.UNIX);
            UnixDomainSocketAddress address = UnixDomainSocketAddress.of(SOCKET_FILE_PATH);
            channel.connect(address);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void disconnect() {
        try {
            this.channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // SERVER RECEIVE
    @Override
    public void start() {
        try {
            ByteBuffer byteBuffer = ByteBuffer.allocate(1);
            ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
            while (connected) {
                byte header = UDSClientHandler.readByte(channel, byteBuffer);
                if (header == -1) {
                    break;
                }

                switch (EPackage.fromByte(header)) {
                    case INIT: {
                        int dataSize = UDSClientHandler.readInt(channel, intBuffer);
                        byte[] data = UDSClientHandler.readFully(channel, dataSize);
                        String message = new String(data, StandardCharsets.UTF_8);
                        String[] parts = message.split(EPackage.STRING_DELIMETER);
                        this.clientIndex = Integer.parseInt(parts[0]);
                        this.alphabetSplit = new String[parts.length - 1];
                        for (int i = 1; i < parts.length; i++) {
                            this.alphabetSplit[i - 1] = parts[i];
                            Utils.associateKeys(this.wordCount, parts[i], new HashMap<>());
                        }
                        this.send(channel, EPackage.CONNECTED, null);
                        break;
                    }
                    case MAP: {
                        int dataSize = UDSClientHandler.readInt(channel, intBuffer);
                        byte[] data = UDSClientHandler.readFully(channel, dataSize);
                        String message = new String(data, StandardCharsets.UTF_8);
                        Utils.countWords(this.wordCount, message);
                        this.send(channel, EPackage.MAP, null);
                        break;
                    }
                    case SHUFFLE: {
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
                            this.wordCount.get(this.alphabetSplit[i]).clear();
                        }
                        this.send(channel, EPackage.SHUFFLE, sj.toString());
                        break;
                    }
                    case REDUCE: {
                        String word;
                        String key;
                        int dataSize = UDSClientHandler.readInt(channel, intBuffer);
                        byte[] data = UDSClientHandler.readFully(channel, dataSize);
                        String message = new String(data, StandardCharsets.UTF_8);
                        String[] parts = message.split(EPackage.STRING_DELIMETER);
                        if (parts.length > 1) {
                            for (int i = 0; i < parts.length; i += 2) {
                                if (i + 1 >= parts.length) {
                                    System.out.println("PACKAGE WENT MISSING");
                                }
                                word = parts[i];
                                key = String.valueOf(word.charAt(0));
                                int count = Integer.parseInt(parts[i + 1]);
                                wordCount.get(key).compute(word, (k, v) -> (v == null) ? count : v + count);
                            }
                        }
                        break;
                    }
                    case MERGE: {
                        StringJoiner sj = new StringJoiner(EPackage.STRING_DELIMETER);
                        for (Map.Entry<String, Integer> entry : this.wordCount
                                .get(this.alphabetSplit[this.clientIndex])
                                .entrySet()) {
                            sj.add(entry.getKey());
                            sj.add(String.valueOf(entry.getValue()));
                        }
                        send(channel, EPackage.MERGE, sj.toString());
                        break;
                    }
                    case DONE:
                        connected = false;
                        this.disconnect();
                        break;
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void send(SocketChannel channel, EPackage header, String message) {
        try {
            switch (header) {
                case CONNECTED:
                case MAP: {
                    UDSServer.writeByte(channel, header.getValue());
                    break;
                }
                case SHUFFLE:
                case MERGE: {
                    UDSServer.writeByte(channel, header.getValue());
                    byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
                    UDSServer.writeInt(channel, bytes.length);
                    UDSServer.write(channel, bytes, bytes.length);
                    //out.flush();
                    break;
                }
                default:
                    break;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
