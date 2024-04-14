package org.betriebssysteme.IPCVariants.ZMQ;

import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.betriebssysteme.Classes.IPCClient;
import org.betriebssysteme.Enum.EPackage;

public class ZMQClient extends IPCClient {
    private ZContext context;
    private ZMQ.Socket socket;
    private HashMap<String, HashMap<String, Integer>> wordCount = new HashMap<>();
    private String[] alphabetSplit;

    @Override
    public void init(Map<String, Object> configMap) {
        context = new ZContext();
        socket = context.createSocket(ZMQ.DEALER);
        socket.setIdentity(((String) (configMap.getOrDefault("clientId", "Client")) + Thread.currentThread().getId()).getBytes());
        socket.connect("tcp://localhost:5555");
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

            processMessage(new String(message, StandardCharsets.UTF_8));
        }
    }

    private void processMessage(String message) {
        String[] parts = message.split(EPackage.STRING_DELIMITER);
        EPackage header = EPackage.fromByte(Byte.parseByte(parts[0]));
        switch (header) {
            case INIT:
                // handle INIT
                break;
            case MAP:
                // handle MAP
                break;
            case SHUFFLE:
                // handle SHUFFLE
                break;
            case REDUCE:
                // handle REDUCE
                break;
            case MERGE:
                // handle MERGE
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
