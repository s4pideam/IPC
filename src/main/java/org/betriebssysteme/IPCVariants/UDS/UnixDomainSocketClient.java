package org.betriebssysteme.IPCVariants.UDS;

import org.betriebssysteme.IPCVariants.TCP.TCPClient;
import org.betriebssysteme.Utils.Utils;

import java.io.*;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Map;

public class UnixDomainSocketClient extends TCPClient {
    SocketChannel channel;
    public UnixDomainSocketClient() {
        this.rapidFlush = true;
    }

    @Override
    public void init(Map<String, Object> configMap) {
        try {
            Path socketPath = FileSystems.getDefault().getPath("./uds/server.socket");
            UnixDomainSocketAddress socketAddress = UnixDomainSocketAddress.of(socketPath);
            channel = SocketChannel.open(StandardProtocolFamily.UNIX);
            channel.connect(socketAddress);

            this.outputStream = new DataOutputStream(Channels.newOutputStream(channel));
            this.inputStream = new DataInputStream(Channels.newInputStream(channel));


        } catch (Exception e) {
            e.printStackTrace();
        }

        connected = true;
    }

    @Override
    public void disconnect() {
        try {
            this.outputStream.close();
            this.inputStream.close();
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
