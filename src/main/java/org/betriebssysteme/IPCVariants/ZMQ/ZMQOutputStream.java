package org.betriebssysteme.IPCVariants.ZMQ;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.io.OutputStream;

public class ZMQOutputStream extends java.io.DataOutputStream {
    private ZMQ.Socket socket;

    public ZMQOutputStream(ZMQ.Socket socket) {
        super(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                socket.send(new byte[]{(byte) b}, 0);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                byte[] data = new byte[len];
                System.arraycopy(b, off, data, 0, len);
                socket.send(data, 0);
            }

            @Override
            public void flush() throws IOException {
                // Flushing might not be needed depending on the ZMQ socket type and configuration.
            }

            @Override
            public void close() throws IOException {
                socket.close();
            }
        });
        this.socket = socket;
    }
}