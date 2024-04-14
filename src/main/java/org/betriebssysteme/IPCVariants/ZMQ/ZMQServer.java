package org.betriebssysteme.IPCVariants.ZMQ;

import org.betriebssysteme.Classes.ClientStatus;
import org.betriebssysteme.Classes.OutputStreamClientHandler;
import org.betriebssysteme.Classes.OutputStreamServer;
import org.betriebssysteme.Enum.EPackage;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Map;

public class ZMQServer extends OutputStreamServer {
    private ZContext context;
    private ZMQ.Socket socket;

    private int PORT;

    public ZMQServer(String filePath) {
        super(filePath);
        this.context = new ZContext();
    }

    @Override
    public void init(Map<String, Object> configMap) {
        this.PORT = (int) configMap.getOrDefault("port", 42069);
        this.CHUNK_SIZE = Math.max((int) configMap.getOrDefault("chunkSize", 32), 32);
        this.CLIENT_NUMBERS = Math.min((int) configMap.getOrDefault("clientNumbers", 2), 24);
        this.offsets = this.getOffsets(CLIENT_NUMBERS, CHUNK_SIZE);
        this.alphabetSplit = this.splitAlphabet(CLIENT_NUMBERS);
        this.socket = context.createSocket(SocketType.ROUTER);
        this.socket.bind("tcp://*:" + PORT);
    }

    @Override
    public void start() {
        int currentIndex = 0;
        try {
            while (currentIndex < this.CLIENT_NUMBERS) {
                // Empfange Identität des Clients und danach die Nachricht
                byte[] clientIdentity = socket.recv(0);
                byte[] request = socket.recv(0);
                DataInputStream clientInputStream = new DataInputStream(new ZMQInputStream(socket));
                DataOutputStream clientOutputStream = new DataOutputStream(new ZMQOutputStream(socket));

                Thread thread = new Thread(
                        new OutputStreamClientHandler(this, clientInputStream, clientOutputStream, offsets.get(currentIndex)));
                String key = String.join("", alphabetSplit.get(currentIndex));
                this.clientQueue.put(key, clientOutputStream);
                this.clientThreads.put(key, thread);
                this.clientStatus.put(clientOutputStream, new ClientStatus());
                this.generateInitMessage(clientOutputStream, currentIndex);
                currentIndex++;
            }

            clientThreads.values().forEach(Thread::start);

            for (Thread thread : clientThreads.values()) {
                thread.join();
            }

            wordCount.entrySet().stream()
                    .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                    .limit(10)
                    .forEach(entry -> System.out.println(entry.getKey() + ": " + entry.getValue()));

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            this.socket.close();
            this.context.close();
        }
    }
}
