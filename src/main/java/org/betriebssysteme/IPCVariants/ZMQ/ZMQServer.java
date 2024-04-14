package org.betriebssysteme.IPCVariants.ZMQ;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import java.util.Map;

import org.betriebssysteme.Classes.IPCServer;

public class ZMQServer extends IPCServer {
    private ZContext context;
    private ZMQ.Socket socket;

    public ZMQServer(String filePath) {
        super(filePath);
    }


    @Override
    public void init(Map<String, Object> configMap) {
        int PORT = (int) configMap.getOrDefault("port", 42069);
        String HOST = (String) configMap.getOrDefault("host", "localhost");
        context = new ZContext();
        socket = context.createSocket(SocketType.ROUTER);
        socket.bind("tcp://"+ HOST + ":" + PORT);
    }

    @Override
    public void start() {
        while (!Thread.currentThread().isInterrupted()) {
            // ZMQ.ROUTER sockets receive the identity of the sender followed by the message
            byte[] clientIdentity = socket.recv(0);
            if (clientIdentity == null) break;
            byte[] request = socket.recv(0);
            if (request == null) break;

            // Process request in a separate thread to maintain non-blocking behavior
            Thread processThread = new Thread(() -> processRequest(clientIdentity, request));
            processThread.start();
        }
    }

    private void processRequest(byte[] clientIdentity, byte[] request) {
        String requestStr = new String(request);
        // Here, parse and handle the request based on your protocol
        // For example, respond back to client
        socket.send(clientIdentity, ZMQ.SNDMORE);
        socket.send("Response based on request", 0);
    }
    public void disconnect() {
        socket.close();
        context.close();
    }
}
