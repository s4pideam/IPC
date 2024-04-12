package org.betriebssysteme.IPCVariants.PIPE;

import org.betriebssysteme.IPCVariants.TCP.TCPClient;

import java.io.*;
import java.util.Map;

public class PipeClient extends TCPClient {

    public PipeClient() {
        this.rapidFlush = true;
    }

    @Override
    public void init(Map<String, Object> configMap) {

        this.outputStream = new DataOutputStream(System.out);
        this.inputStream = new DataInputStream(System.in);
        connected = true;
    }


    @Override
    public void disconnect() {
        try {
            this.outputStream.close();
            this.inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
