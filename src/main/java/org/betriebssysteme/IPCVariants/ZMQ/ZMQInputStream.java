package org.betriebssysteme.IPCVariants.ZMQ;

import java.io.IOException;
import java.io.InputStream;
import org.zeromq.ZMQ;

public class ZMQInputStream extends InputStream {
    private ZMQ.Socket socket;
    private byte[] clientIdentity;
    private byte[] buffer;
    private int bufferIndex = 0;

    public ZMQInputStream(ZMQ.Socket socket, byte[] clientIdentity) {
        this.socket = socket;
        this.clientIdentity = clientIdentity;
    }

    @Override
    public int read() throws IOException {
        if (buffer == null || bufferIndex >= buffer.length) {
            buffer = socket.recv(0);
            bufferIndex = 0;
            if (buffer == null || buffer.length == 0) {
                return -1; // Keine Daten verfügbar
            }
        }
        return buffer[bufferIndex++] & 0xFF;
    }
}
