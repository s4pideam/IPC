package org.betriebssysteme.IPCVariants.UDS;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.StringJoiner;

import org.betriebssysteme.Classes.ClientStatus;
import org.betriebssysteme.Enum.EPackage;
import org.betriebssysteme.Record.Offsets;

public class UDSClientHandler extends Thread {
    private final UDSServer server;
    private final SocketChannel channel;
    private final List<Offsets> offsets;
    private int currentOffset = 0;

    public UDSClientHandler(UDSServer server, SocketChannel channel,
            List<Offsets> offsets) {

        this.server = server;
        this.channel = channel;
        this.offsets = offsets;
    }

    // RECEIVING from client
    @Override
    public void run() {
        try {
            ByteBuffer byteBuffer = ByteBuffer.allocate(1);
            ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
            // synchronized (this.server) {
            while (!this.server.clientStatus.get(channel).FINISHED) {
                byte header = readByte(channel, byteBuffer);
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
                        this.server.send(this.channel, EPackage.MAP, message);
                        currentOffset++;
                    } else {
                        this.server.clientStatus.get(channel).MAPPED = true;
                        this.server.send(this.channel, EPackage.SHUFFLE, null);
                    }
                    break;
                }
                case SHUFFLE: {
                    int dataSize = readInt(channel, intBuffer);
                    byte[] data = readFully(channel, dataSize);
                    String message = new String(data, StandardCharsets.UTF_8);
                    String[] parts = message.split(EPackage.STRING_DELIMITER);
                    int i = 0;
                    if (parts.length > 1) {
                        while (i < parts.length) {
                            String key = parts[i];
                            StringJoiner sj = new StringJoiner(EPackage.STRING_DELIMITER);
                            int count = Integer.parseInt(parts[i + 1]) * 2;
                            i = i + 2;
                            count = count + i;
                            while (i < count) {
                                sj.add(parts[i]);
                                sj.add(parts[i + 1]);
                                i = i + 2;
                            }
                            if (server.clientQueue.containsKey(key)) {
                                SocketChannel clientChannel = server.clientQueue.get(key);
                                this.server.send(clientChannel, EPackage.REDUCE, sj.toString());
                            }
                        }
                    }

                    ClientStatus clientstatus = this.server.clientStatus.get(channel);
                    if ((this.server.CLIENT_NUMBERS == 1)
                            || (clientstatus.MAPPED
                                    && clientstatus.REDUCED == this.server.CLIENT_NUMBERS - 1)) {
                        this.server.send(this.channel, EPackage.MERGE, null);

                    }
                    break;
                }
                case MERGE: {
                    int dataSize = readInt(channel, intBuffer);
                    byte[] data = readFully(channel, dataSize);
                    String message = new String(data, StandardCharsets.UTF_8);
                    String[] parts = message.split(EPackage.STRING_DELIMITER);
                    for (int i = 0; i < parts.length - 1; i = i + 2) {
                        String word = parts[i];
                        int count = Integer.parseInt(parts[i + 1]);
                        this.server.updateWordCount(word, count);
                    }
                    this.server.clientStatus.get(channel).MERGED = true;
                    this.server.send(this.channel, EPackage.DONE, null);
                    this.server.clientStatus.get(channel).FINISHED = true;
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

    public static int readInt(SocketChannel channel, ByteBuffer buffer) throws IOException{
        buffer.clear();
        channel.read(buffer);
        buffer.flip();
        return buffer.getInt();
    }

    public static byte readByte(SocketChannel channel, ByteBuffer buffer) throws IOException{
        buffer.clear();
        channel.read(buffer);
        buffer.flip();
        return buffer.get();
    }

    public static byte[] readFully(SocketChannel channel, int bytes) throws IOException{
        ByteBuffer buffer = ByteBuffer.allocate(bytes);
        while(buffer.hasRemaining()){
            channel.read(buffer);
        }
        return buffer.array();
    }
}