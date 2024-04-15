package org.betriebssysteme.IPCVariants.ZMQ;

import org.betriebssysteme.Classes.IPCServer;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.betriebssysteme.Enum.EPackage;

public class ZMQServer extends IPCServer {
    private ZContext context;
    private ZMQ.Socket socket;

    public ZMQServer(String filePath) {
        super(filePath);
    }

    @Override
    public void init(Map<String, Object> configMap) {
        int PORT = 5555;//(int) configMap.getOrDefault("port", 5555);
        String HOST = "localhost";//(String) configMap.getOrDefault("host", "localhost");
        context = new ZContext();
        socket = context.createSocket(SocketType.ROUTER);
        socket.bind("tcp://" + HOST + ":" + PORT);
    }

    @Override
    public void start() {
        while (!Thread.currentThread().isInterrupted()) {
            byte[] clientIdentity = socket.recv(0);
            System.out.println("received clientidentity");
            if (clientIdentity == null){
                System.out.println("ClientIdentity null");
                break;
            }
            byte[] request = socket.recv(0);
            System.out.println("received request");
            if (request == null) {
                System.out.println("Request null");
                break;
            }

            Thread processThread = new Thread(() -> processRequest(clientIdentity, request));
            processThread.start();
        }
    }

    private synchronized void processRequest(byte[] clientIdentity, byte[] request) {
        String requestStr = new String(request, StandardCharsets.UTF_8);
        String[] parts = requestStr.split(EPackage.STRING_DELIMITER);
        System.out.println("RequestStr: " +requestStr);
        EPackage header = EPackage.fromByte(Byte.parseByte(parts[0]));
        switch (header) {
            case INIT:
                System.out.println("Server received init");
                //send(clientIdentity, EPackage.INIT, null);
                break;
            case CONNECTED:
                System.out.println("Client connected");
                break;
            case MAP:
                System.out.println("Client finished mapping: " + parts[1]);
                break;
            case SHUFFLE:
                send(clientIdentity, EPackage.REDUCE, "Shuffle data received");
                break;
            case REDUCE:
                System.out.println("Reduction done by client");
                break;
            case MERGE:
                System.out.println("Merge complete: " + parts[1]);
                break;
            case DONE:
                send(clientIdentity, EPackage.DONE, "Processing complete");
                break;
            default:
                System.err.println("Unknown message type received: " + header);
                break;
        }
    }

    private void send(byte[] clientIdentity, EPackage header, String message) {
        socket.send(clientIdentity, ZMQ.SNDMORE);
        socket.send(header.getValue() + EPackage.STRING_DELIMITER + message, 0);
    }
}
