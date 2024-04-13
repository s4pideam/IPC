package org.betriebssysteme.IPCVariants.TCP;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import org.betriebssysteme.Classes.OutputStreamClient;


public class TCPClient extends OutputStreamClient {
    private Socket socket;

    public TCPClient(){}

    @Override
    public void init(Map<String, Object> configMap) {
        int PORT = (int) configMap.getOrDefault("port", 42069);
        String HOST = (String) configMap.getOrDefault("host", "localhost");
        try {
            this.socket = new Socket(HOST, PORT);
            this.inputStream = new DataInputStream(this.socket.getInputStream());
            this.outputStream = new DataOutputStream(this.socket.getOutputStream());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void disconnect() {
        try {
            this.socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
