package org.betriebssysteme.IPCVariants.ZMQ;

import java.io.IOException;
import java.io.OutputStream;
import org.zeromq.ZMQ;

public class ZMQOutputStream extends OutputStream {
    private ZMQ.Socket socket;
    private byte[] clientIdentity;

    public ZMQOutputStream(ZMQ.Socket socket, byte[] clientIdentity) {
        this.socket = socket;
        this.clientIdentity = clientIdentity;
    }

    @Override
    public void write(int b) throws IOException {
        write(new byte[]{(byte) b}, 0, 1);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        socket.sendMore(clientIdentity);
        byte[] data = new byte[len];
        System.arraycopy(b, off, data, 0, len);
        socket.send(data, 0);
    }
}
