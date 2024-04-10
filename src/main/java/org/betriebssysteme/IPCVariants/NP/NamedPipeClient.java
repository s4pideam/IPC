package org.betriebssysteme.IPCVariants.NP;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import org.betriebssysteme.Enum.EPackage;
import org.betriebssysteme.Interfaces.IIPCClient;
import org.betriebssysteme.Interfaces.ISendable;
import org.betriebssysteme.Utils.Utils;

public class NamedPipeClient implements IIPCClient, ISendable<DataOutputStream> {
    private Path serverToClient;
    private Path clientToServer;
    private DataInputStream inputStream;
    private DataOutputStream outputStream;
    private boolean connected = false;

    private final HashMap<String, HashMap<String, Integer>> wordCount = new HashMap<>();
    private int clientIndex = 0;
    private String[] alphabetSplit;

    public NamedPipeClient() throws FileNotFoundException {
    }

    @Override
    public void init(Map<String, Object> configMap) {
        int id = (int)configMap.get("id");
        serverToClient = FileSystems.getDefault().getPath("./np/server_to_client" + id);
        clientToServer = FileSystems.getDefault().getPath("./np/client_to_server" + id);

        Utils.createNamedPipe(serverToClient);
        Utils.createNamedPipe(clientToServer);

        try {
            outputStream = new DataOutputStream(new FileOutputStream(clientToServer.toString()));
            inputStream = new DataInputStream(new FileInputStream(serverToClient.toString()));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        connected = true;
    }

    @Override
    public void disconnect() {
        try {
            this.outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void start() {
        if (!connected) {
            System.err.println("Something went wrong initializing the named pipe client.");
        }
        try {
            while (connected) {
                byte header = inputStream.readByte();
                if (header == -1) {
                    break;
                }

                switch (EPackage.fromByte(header)) {
                case INIT: {
                    int dataSize = this.inputStream.readInt();
                    byte[] data = new byte[dataSize];
                    this.inputStream.readFully(data);
                    String message = new String(data, StandardCharsets.UTF_8);

                    String[] parts = message.split(EPackage.STRING_DELIMETER);
                    this.clientIndex = Integer.parseInt(parts[0]);
                    this.alphabetSplit = new String[parts.length - 1];
                    for (int i = 1; i < parts.length; i++) {
                        this.alphabetSplit[i - 1] = parts[i];
                        Utils.associateKeys(this.wordCount, parts[i], new HashMap<>());
                    }
                    this.send(this.outputStream, EPackage.CONNECTED, null);
                    break;
                }
                case MAP: {
                    int dataSize = this.inputStream.readInt();
                    byte[] data = new byte[dataSize];
                    this.inputStream.readFully(data);
                    String message = new String(data, StandardCharsets.UTF_8);
                    Utils.countWords(this.wordCount, message);
                    this.send(this.outputStream, EPackage.MAP, null);
                    break;
                }
                case SHUFFLE: {
                    StringJoiner sj = new StringJoiner(EPackage.STRING_DELIMETER);
                    for (int i = 0; i < this.alphabetSplit.length; i++) {
                        if (this.clientIndex == i) {
                            continue;
                        }
                        sj.add(this.alphabetSplit[i]);
                        sj.add(String.valueOf(this.wordCount.get(this.alphabetSplit[i]).size()));
                        for (String key : this.wordCount.get(this.alphabetSplit[i]).keySet()) {
                            sj.add(key);
                            sj.add(String.valueOf(this.wordCount.get(this.alphabetSplit[i]).get(key)));
                        }
                        this.wordCount.get(this.alphabetSplit[i]).clear();
                    }
                    this.send(this.outputStream, EPackage.SHUFFLE, sj.toString());
                    break;
                }
                case REDUCE: {
                    String word;
                    String key;
                    int dataSize = this.inputStream.readInt();
                    byte[] data = new byte[dataSize];
                    this.inputStream.readFully(data);
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
                    send(this.outputStream, EPackage.MERGE, sj.toString());
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

    @Override
    public void send(DataOutputStream out, EPackage header, String message) {
        try {
            switch (header) {
            case CONNECTED:
            case MAP: {
                out.write(header.getValue());
                break;
            }
            case SHUFFLE:
            case MERGE: {
                out.writeByte(header.getValue());
                byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
                out.writeInt(bytes.length);
                out.write(bytes);
                out.flush();
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
