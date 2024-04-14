package org.betriebssysteme.IPCVariants.ZMQ;

import org.betriebssysteme.Classes.OutputStreamClient;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Map;

public class ZMQClient extends OutputStreamClient {
    private ZContext context;
    private ZMQ.Socket socket;

    public ZMQClient() {}

    @Override
    public void init(Map<String, Object> configMap) {
        String address = (String) configMap.getOrDefault("address", "tcp://localhost:42069");
        try {
            this.context = new ZContext();
            this.socket = context.createSocket(SocketType.DEALER);
            this.socket.connect(address);
            this.outputStream = new DataOutputStream(new ZMQOutputStream(socket));
            this.inputStream = new DataInputStream(new ZMQInputStream(socket));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void disconnect() {
        if (socket != null) {
            socket.close();
            socket = null;
        }
        if (context != null) {
            context.close();
            context = null;
        }
    }


}