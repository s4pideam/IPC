package org.betriebssysteme.Classes;

import org.betriebssysteme.Enum.EPackage;
import org.betriebssysteme.Record.Offsets;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.StringJoiner;

public class OutputStreamClientHandler extends Thread {
    private final IPCServer server;
    private final DataInputStream inputStream;
    private final DataOutputStream outputStream;
    private final List<Offsets> offsets;
    private int currentOffset = 0;

    public OutputStreamClientHandler(IPCServer server, DataInputStream dataInputStream, DataOutputStream dataOutputStream,
                         List<Offsets> offsets) {

        this.server = server;
        this.inputStream = dataInputStream;
        this.outputStream = dataOutputStream;
        this.offsets = offsets;
    }

    // RECEIVING from client
    @Override
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
                    case MERGE: {
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
