package org.betriebssysteme.IPCVariants.ZMQ;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.betriebssysteme.Classes.IPCClient;
import org.betriebssysteme.Enum.EPackage;
import org.betriebssysteme.Utils.Utils;

public class ZMQClient extends IPCClient {
    private ZContext context;
    private ZMQ.Socket socket;
    private HashMap<String, HashMap<String, Integer>> wordCount = new HashMap<>();
    private String[] alphabetSplit;

    @Override
    public void init(Map<String, Object> configMap) {
        context = new ZContext();
        socket = context.createSocket(SocketType.DEALER);
        socket.setImmediate(true);
        socket.setIdentity(((String) configMap.getOrDefault("clientId", "Client") + Thread.currentThread().getId()).getBytes());
        socket.connect("tcp://localhost:5555");
        try {
            Thread.sleep(1000);  // Warte 1 Sekunde
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        this.send(EPackage.INIT, "Bitte helfen sie mir");
        System.out.println("sent init");
    }

    @Override
    public void disconnect() {
        socket.close();
        context.close();
    }

    @Override
    public void start() {
        while (!Thread.currentThread().isInterrupted()) {
            byte[] message = socket.recv(0);
            if (message == null) break;
            System.out.println("Received Message, trying to process");
            processMessage(new String(message, StandardCharsets.UTF_8));
        }
    }

    private void processMessage(String message) {
        String[] parts = message.split(EPackage.STRING_DELIMITER);
        EPackage header = EPackage.fromByte(Byte.parseByte(parts[0]));
        switch (header) {
            case INIT:
                System.out.println("Received Init");
                //this.clientIndex = Integer.parseInt(parts[0]);
                this.alphabetSplit = new String[parts.length - 1];
                for (int i = 1; i < parts.length; i++) {
                    this.alphabetSplit[i - 1] = parts[i];
                    Utils.associateKeys(this.wordCount, parts[i], new HashMap<>());
                }
                this.send(EPackage.CONNECTED, null);
                break;
            case MAP:
                // Counting words as specified by MAP command
                Utils.countWords(this.wordCount, parts[1]);
                send(EPackage.MAP, "");
                break;
            case SHUFFLE:
                // Implement shuffle logic here
                break;
            case REDUCE:
                // Implement reduce logic here
                break;
            case MERGE:
                // Implement merge logic here
                break;
            case DONE:
                disconnect();
                return;
            default:
                break;
        }
    }

    public void send(EPackage header, String message) {
        socket.send(header.getValue() + EPackage.STRING_DELIMITER + message, 0);
    }
}
