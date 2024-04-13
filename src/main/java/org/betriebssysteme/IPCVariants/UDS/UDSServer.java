package org.betriebssysteme.IPCVariants.UDS;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.betriebssysteme.Classes.ClientStatus;
import org.betriebssysteme.Classes.IPCServer;
import org.betriebssysteme.Enum.EPackage;
import org.betriebssysteme.Record.Offsets;

public class UDSServer extends IPCServer{
    protected int CHUNK_SIZE;

    private List<List<Offsets>> offsets;

    private final Path SOCKET_FILE_PATH = Path.of("/tmp/uds_server.sock");
    
    public ConcurrentHashMap<String, SocketChannel> clientQueue = new ConcurrentHashMap<>();
    public ConcurrentHashMap<SocketChannel, ClientStatus> clientStatus = new ConcurrentHashMap<>();

    public UDSServer(String filePath) {
        super(filePath);
        try {
            Files.deleteIfExists(SOCKET_FILE_PATH);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void init(Map<String, Object> configMap) {
        this.CHUNK_SIZE = Math.max((int) configMap.getOrDefault("chunkSize", 32), 32);
        this.CLIENT_NUMBERS = Math.min((int) configMap.getOrDefault("clientNumbers", 2), 24);
        this.offsets = this.getOffsets(CLIENT_NUMBERS, CHUNK_SIZE);
        this.alphabetSplit = this.splitAlphabet(CLIENT_NUMBERS);
    }

    @Override
    public void start() {
        int currentIndex = 0;
        try (ServerSocketChannel serverChannel = ServerSocketChannel.open(StandardProtocolFamily.UNIX)) {
            UnixDomainSocketAddress address = UnixDomainSocketAddress.of(SOCKET_FILE_PATH);
            serverChannel.bind(address);
            //System.out.println("Server listening on "+SOCKET_FILE_PATH);

            while (currentIndex < this.CLIENT_NUMBERS) {
                SocketChannel clientChannel = serverChannel.accept();
                Thread thread = new Thread(
                        new UDSClientHandler(this, clientChannel, offsets.get(currentIndex)));
                String key = String.join("", alphabetSplit.get(currentIndex));
                this.clientQueue.put(key, clientChannel);
                this.clientThreads.put(key, thread);
                this.clientStatus.put(clientChannel, new ClientStatus());
                this.generateInitMessage(clientChannel, currentIndex);
                currentIndex++;
            }

            clientThreads.values().forEach(Thread::start);

            for (Thread thread : clientThreads.values()) {
                thread.join();
            }

            serverChannel.close();

            wordCount.entrySet().stream()
                    .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                    .limit(10)
                    .forEach(entry -> System.out.println(entry.getKey() + ": " + entry.getValue()));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void generateInitMessage(SocketChannel channel, int index) {
        String result = index + EPackage.STRING_DELIMETER +
                alphabetSplit.stream()
                        .map(innerList -> String.join("", innerList))
                        .collect(Collectors.joining(EPackage.STRING_DELIMETER));
        this.send(channel, EPackage.INIT, result);

    }

    public void send(SocketChannel channel, EPackage header, String message) {
        try {
            synchronized (channel) {
                switch (header) {
                case INIT:
                case MAP:
                case REDUCE: {
                    writeByte(channel, header.getValue());
                    byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
                    writeInt(channel, bytes.length);
                    write(channel, bytes, bytes.length);
                    if (header == EPackage.REDUCE) {
                        synchronized (this) {
                            ClientStatus clientStatus = this.clientStatus.get(channel);
                            clientStatus.REDUCED++;
                            if ((clientStatus.REDUCED == this.CLIENT_NUMBERS - 1) && (clientStatus.MAPPED)
                                    && (!clientStatus.MERGED)) {
                                writeByte(channel, EPackage.MERGE.getValue());
                            }
                        }
                    }
                    if (header == EPackage.INIT) {
                        //channel.flush();
                    }
                    break;
                }
                case SHUFFLE:
                case MERGE:
                case CONNECTED:
                case DONE: {
                    writeByte(channel, header.getValue());
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
    @Override
    public synchronized void updateWordCount(String word, int count) {
        wordCount.compute(word, (k, v) -> (v == null) ? count : v + count);
    }

    public static int writeInt(SocketChannel channel, int data) throws IOException{
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(data);
        buffer.flip();
        return channel.write(buffer);
    }

    public static int writeByte(SocketChannel channel, byte data) throws IOException{
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(data);
        buffer.flip();
        return channel.write(buffer);
    }

    public static void write(SocketChannel channel, byte[] data, int dataLength) throws IOException{
        ByteBuffer buffer = ByteBuffer.allocate(dataLength);
        buffer.put(data);
        buffer.flip();
        while(buffer.hasRemaining()){
            channel.write(buffer);
        }
    }
}
