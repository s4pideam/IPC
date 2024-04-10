package org.betriebssysteme;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.betriebssysteme.IPCVariants.NP.NamedPipeClient;
import org.betriebssysteme.IPCVariants.NP.NamedPipeServer;
import org.betriebssysteme.IPCVariants.TCP.TCPClient;
import org.betriebssysteme.IPCVariants.TCP.TCPServer;

public class App {

    public static void main(String[] args) throws IOException {
        String FILE_PATH = "./mobydick.txt";
        String HOST = "localhost";
        int CHUNK_SIZE = 20000;
        int PORT = 42069;
        int CLIENT_NUMBERS = 2;

        try {
            if (args.length < 2) {
                throw new IllegalArgumentException();
            }
            switch (args[1]) {
            case "tcp": {
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
                    client.start();
                } else {
                    throw new IllegalArgumentException();
                }
            }
            case "np": {
                if (args[0].equals("s")) {
                    NamedPipeServer server = new NamedPipeServer(FILE_PATH);
                    Map<String, Object> configMap = new HashMap<>();
                    configMap.put("chunkSize", CHUNK_SIZE);
                    configMap.put("clientNumbers", CLIENT_NUMBERS);
                    server.init(configMap);
                    server.start();
                } else if (args[0].equals("c")) {
                    NamedPipeClient client = new NamedPipeClient();
                    if(args.length != 3){
                        System.out.println("Third argument: id of the client");
                    }
                    Map<String, Object> configMap = new HashMap<>();
                    configMap.put("id", Integer.parseInt(args[2]));
                    client.init(configMap);
                    client.start();
                } else {
                    throw new IllegalArgumentException();
                }
            }
            }

        } catch (IllegalArgumentException e) {
            System.out.println("First argument: Please provide 's' for server or 'c' for client.\n"
                    + "Second argument: Please provide 'tcp' or 'np'");
        }
    }

}
