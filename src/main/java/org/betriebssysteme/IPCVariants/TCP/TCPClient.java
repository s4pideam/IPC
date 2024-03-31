package org.betriebssysteme.IPCVariants.TCP;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import org.betriebssysteme.Enum.EClientStatus;
import org.betriebssysteme.Enum.EPackage;
import org.betriebssysteme.Interfaces.IIPCClient;
import org.betriebssysteme.Interfaces.ISendable;

public class TCPClient implements IIPCClient, ISendable<DataOutputStream> {
    private Socket socket;
    private DataOutputStream outputStream;
    private DataInputStream inputStream;
    private EClientStatus eClientStatus = EClientStatus.WORKING;
    private int PORT;
    private String HOST;

    private HashMap<String, HashMap<String, Integer>> wordCount = new HashMap<>();
    private int clientIndex = 0;
    private String[] alphabetSplit;

    private void associateKeys(String keys, HashMap<String, HashMap<String, Integer>> hashMap,
            HashMap<String, Integer> object) {
        hashMap.put(keys, object);
        for (char key : keys.toCharArray()) {
            hashMap.put(String.valueOf(key), object);
        }
    }

    private void countWords(String message) {
        String[] words = message.toLowerCase().split("\\W+");

        for (String word : words) {
            if (!word.matches("[a-z]+"))
                continue;
            String key = String.valueOf(word.charAt(0));
            wordCount.computeIfAbsent(key, k -> new HashMap<>());
            wordCount.get(key).merge(word, 1, Integer::sum);
        }
    }

    // SERVER RECEIVE
    @Override
    public void connect() {
        try {
            this.socket = new Socket(this.HOST, this.PORT);
            this.inputStream = new DataInputStream(this.socket.getInputStream());
            this.outputStream = new DataOutputStream(this.socket.getOutputStream());

            int dataSize;
            byte[] data;
            String message;

            while (eClientStatus != EClientStatus.DONE) {
                byte header = inputStream.readByte();
                if (header == -1) {
                    break;
                }

                switch (EPackage.fromByte(header)) {
                    case INIT:
                        dataSize = this.inputStream.readInt();
                        data = new byte[dataSize];
                        this.inputStream.readFully(data);
                        message = new String(data, StandardCharsets.UTF_8);

                        String[] parts = message.split(EPackage.STRING_DELIMETER);
                        StringJoiner sj = new StringJoiner(EPackage.STRING_DELIMETER);
                        this.clientIndex = Integer.parseInt(parts[0]);
                        this.alphabetSplit = new String[parts.length - 1];
                        for (int i = 1; i < parts.length; i++) {
                            this.alphabetSplit[i - 1] = parts[i];
                            this.associateKeys(parts[i], this.wordCount, new HashMap<>());
                        }
                        this.send(this.outputStream, EPackage.CONNECTED, null);
                        break;
                    case MAP:
                        dataSize = this.inputStream.readInt();
                        data = new byte[dataSize];
                        this.inputStream.readFully(data);
                        message = new String(data, StandardCharsets.UTF_8);
                        this.countWords(message);
                        this.send(this.outputStream, EPackage.MAP, null);
                        break;
                    case SHUFFLE:
                        sj = new StringJoiner(EPackage.STRING_DELIMETER);
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
                        // System.out.println(sj.toString());
                        this.send(this.outputStream, EPackage.SHUFFLE, sj.toString());
                        break;
                    case REDUCE:
                        String word;
                        String key;
                        dataSize = this.inputStream.readInt();
                        data = new byte[dataSize];
                        this.inputStream.readFully(data);
                        message = new String(data, StandardCharsets.UTF_8);
                        parts = message.split(EPackage.STRING_DELIMETER);
                        if (parts.length > 1) {
                            for (int i = 0; i < parts.length; i += 2) {
                                if (i + 1 >= parts.length) {
                                    // System.out.println("what?");
                                }
                                word = parts[i];
                                key = String.valueOf(word.charAt(0));
                                int count = Integer.parseInt(parts[i + 1]);
                                wordCount.get(key).compute(word, (k, v) -> (v == null) ? count : v + count);
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
                        send(this.outputStream, EPackage.MERGE, sj.toString());
                        break;
                    case DONE:
                        eClientStatus = EClientStatus.DONE;
                        this.disconnect();
                        break;
                    default:
                        // Handle default case
                        break;
                }
                // System.out.println("Client[" + this.clientIndex + "] last package: " +
                // lastHeader);
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
    public void send(DataOutputStream out, EPackage header, String message) {
        try {
            switch (header) {
                case CONNECTED:
                case MAP:
                    out.write(header.getValue());
                    break;
                case SHUFFLE:
                case MERGE:
                    out.writeByte(header.getValue());
                    byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
                    out.writeInt(bytes.length);
                    out.write(bytes);
                    break;
                default:
                    break;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
