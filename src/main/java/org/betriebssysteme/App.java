package org.betriebssysteme;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.betriebssysteme.IPCVariants.TCP.TCPServer;
import org.betriebssysteme.IPCVariants.TCP.TCPClient;

public class App {

    public static void main(String[] args) throws IOException {
        String FILE_PATH = "./mobydick.txt";
        String HOST = "localhost";
        int CHUNK_SIZE = 4096;
        int PORT = 42069;
        int CLIENT_NUMBERS = 3;

        if (args.length > 0) {
            if (args[0].equals("s")) {
                TCPServer server = new TCPServer(FILE_PATH);
                Map<String, Object> configMap = new HashMap<>();
                configMap.put("host", HOST);
                configMap.put("port", PORT);
                configMap.put("chunkSize", CHUNK_SIZE);
                configMap.put("clientNumbers", CLIENT_NUMBERS);           
                server.init(configMap);
                server.start();
            } else if (args[0].equals("c")) {
                TCPClient client = new TCPClient();
                Map<String, Object> configMap = new HashMap<>();
                configMap.put("host", HOST);
                configMap.put("port", PORT);
                client.init(configMap);
                client.connect();
            } else {
                System.out.println("Invalid argument. Please provide 's' for server or 'c' for client.");
            }
        } else {
            System.out.println("No argument provided. Please provide 's' for server or 'c' for client.");
        }
    }


}



