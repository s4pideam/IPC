package org.betriebssysteme.IPCVariants.ZMQ;

import org.zeromq.ZMQ;

import java.io.IOException;
import java.io.InputStream;

public class ZMQInputStream extends java.io.DataInputStream {
    private ZMQ.Socket socket;

    public ZMQInputStream(ZMQ.Socket socket) {
        super(new InputStream() {
            @Override
            public int read() throws IOException {
                byte[] buffer = socket.recv(0);
                if (buffer == null || buffer.length == 0) {
                    return -1;
                }
                return buffer[0] & 0xFF;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                byte[] data = socket.recv(0);
                if (data == null) {
                    return -1;
                }
                int bytesRead = Math.min(len, data.length);
                System.arraycopy(data, 0, b, off, bytesRead);
                return bytesRead;
            }
        });
        this.socket = socket;
    }
}