package org.betriebssysteme.IPCVariants.NP;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.betriebssysteme.Classes.ClientHandler;
import org.betriebssysteme.Classes.ClientStatus;
import org.betriebssysteme.Classes.IPCServer;
import org.betriebssysteme.Enum.EPackage;
import org.betriebssysteme.Interfaces.ISendable;
import org.betriebssysteme.Record.Offsets;
import org.betriebssysteme.Utils.Utils;

public class NamedPipeServer extends IPCServer implements ISendable<DataOutputStream> {
    private int CHUNK_SIZE;

    private List<List<Offsets>> offsets;

    public NamedPipeServer(String filePath) {
        super(filePath);

    }

    private Path[] serverToClientNamedPipes;
    private Path[] clientToServerNamedPipes;

    @Override
    public void init(Map<String, Object> configMap) {
        this.CHUNK_SIZE = Math.max((int) configMap.getOrDefault("chunkSize", 32), 32);
        this.CLIENT_NUMBERS = Math.min((int) configMap.getOrDefault("clientNumbers", 2), 24);
        this.offsets = this.getOffsets(CLIENT_NUMBERS, CHUNK_SIZE);
        this.alphabetSplit = this.splitAlphabet(CLIENT_NUMBERS);

        serverToClientNamedPipes = new Path[CLIENT_NUMBERS];
        clientToServerNamedPipes = new Path[CLIENT_NUMBERS];

        for (int i = 0; i < this.CLIENT_NUMBERS; i++) {
            // init named pipes
            Path serverToClient = FileSystems.getDefault().getPath("./np/server_to_client" + i);
            serverToClientNamedPipes[i] = serverToClient;
            Path clientToServer = FileSystems.getDefault().getPath("./np/client_to_server" + i);
            clientToServerNamedPipes[i] = clientToServer;
            Utils.createNamedPipe(serverToClient);
            Utils.createNamedPipe(clientToServer);
        }
    }

    @Override
    public void start() {
        int currentIndex = 0;
        try {
            while (currentIndex < this.CLIENT_NUMBERS) {
                DataInputStream clientInputStream = new DataInputStream(
                        new FileInputStream(clientToServerNamedPipes[currentIndex].toString()));
                DataOutputStream clientOutputStream = new DataOutputStream(
                        new FileOutputStream(serverToClientNamedPipes[currentIndex].toString()));
                Thread thread = new Thread(
                        new ClientHandler(this, clientInputStream, clientOutputStream, offsets.get(currentIndex)));
                String key = String.join("", alphabetSplit.get(currentIndex));
                this.clientQueue.put(key, clientOutputStream);
                this.clientThreads.put(key, thread);
                this.clientStatus.put(clientOutputStream, new ClientStatus());
                this.generateInitMessage(clientOutputStream, currentIndex);
                currentIndex++;
            }
        } catch (IOException e) {
            System.err.println(e);
        }
        ;

        clientThreads.values().forEach(Thread::start);

        try {
            for (Thread thread : clientThreads.values()) {
                thread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        wordCount.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(10)
                .forEach(entry -> System.out.println(entry.getKey() + ": " + entry.getValue()));

    }

    private void generateInitMessage(DataOutputStream out, int index) {
        String result = index + EPackage.STRING_DELIMETER +
                alphabetSplit.stream()
                        .map(innerList -> String.join("", innerList))
                        .collect(Collectors.joining(EPackage.STRING_DELIMETER));
        this.send(out, EPackage.INIT, result);

    }

    @Override
    public void send(DataOutputStream out, EPackage header, String message) {
        try {
            synchronized (out) {
                switch (header) {
                case INIT:
                case MAP:
                case REDUCE: {
                    out.writeByte(header.getValue());
                    byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
                    out.writeInt(bytes.length);
                    out.write(bytes);
                    if (header == EPackage.REDUCE) {
                        synchronized (this) {
                            ClientStatus clientStatus = this.clientStatus.get(out);
                            clientStatus.REDUCED++;
                            if ((clientStatus.REDUCED == this.CLIENT_NUMBERS - 1) && (clientStatus.MAPPED)
                                    && (!clientStatus.MERGED)) {
                                out.writeByte(EPackage.MERGE.getValue());
                            }
                        }
                    }
                    if (header == EPackage.INIT) {
                        out.flush();
                    }
                    break;
                }
                case SHUFFLE:
                case MERGE:
                case CONNECTED:
                case DONE: {
                    out.writeByte(header.getValue());
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

}