package org.betriebssysteme;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.betriebssysteme.Classes.StreamGobbler;
import org.betriebssysteme.IPCVariants.NP.NamedPipeClient;
import org.betriebssysteme.IPCVariants.NP.NamedPipeServer;
import org.betriebssysteme.IPCVariants.TCP.TCPClient;
import org.betriebssysteme.IPCVariants.TCP.TCPServer;

public class App {

    public static void main(String[] args) throws IOException {
        try {
            String HOST = "localhost";
            int PORT = 42069;
            int CLIENT_NUMBERS;
            int CHUNK_SIZE;
            String FILE_PATH;


            String basePath = System.getProperty("user.dir");
            String jarPath = basePath + File.separator + "target" + File.separator + "ipc.jar";

            if (args.length < 2) {
                throw new IllegalArgumentException();
            }



            switch (args[0]) {
                case "tcp": {
                    if (args[1].equals("s")) {

                        CLIENT_NUMBERS = Integer.parseInt(args[2]);
                        CHUNK_SIZE = Integer.parseInt(args[3]);
                        FILE_PATH = args[4];

                        TCPServer server = new TCPServer(FILE_PATH);
                        Map<String, Object> configMap = new HashMap<>();
                        configMap.put("host", HOST);
                        configMap.put("port", PORT);
                        configMap.put("chunkSize", CHUNK_SIZE);
                        configMap.put("clientNumbers", CLIENT_NUMBERS);
                        server.init(configMap);

                        Thread mainThread = new Thread(server::start);
                        mainThread.start();

                        List<Process> processList = new ArrayList<>();
                        for (int i = 0; i < CLIENT_NUMBERS; i++) {
                            ProcessBuilder processBuilder = new ProcessBuilder("java", "-jar", jarPath, "tcp", "c");
                            Process process = processBuilder.start();
                            processList.add(process);
                            //StreamGobbler outputGobbler = new StreamGobbler(process.getInputStream(), "OUTPUT_SERVER " + String.valueOf(i));
                            //outputGobbler.start();
                        }
                        for (Process process : processList) {
                            process.waitFor();
                        }
                        mainThread.join();
                    }
                    if ((args[1].equals("c"))) {
                        System.out.println("start");
                        TCPClient client = new TCPClient();
                        Map<String, Object> configMap = new HashMap<>();
                        configMap.put("host", HOST);
                        configMap.put("port", PORT);
                        client.init(configMap);
                        client.start();
                    }
                    break;
                }
                case "np": {
                    if (args[1].equals("s")) {

                        CLIENT_NUMBERS = Integer.parseInt(args[2]);
                        CHUNK_SIZE = Integer.parseInt(args[3]);
                        FILE_PATH = args[4];

                        TCPServer server = new NamedPipeServer(FILE_PATH);
                        Map<String, Object> configMap = new HashMap<>();
                        configMap.put("chunkSize", CHUNK_SIZE);
                        configMap.put("clientNumbers", CLIENT_NUMBERS);
                        server.init(configMap);

                        Thread mainThread = new Thread(server::start);
                        mainThread.start();

                        List<Process> processList = new ArrayList<>();
                        for (int i = 0; i < CLIENT_NUMBERS; i++) {
                            ProcessBuilder processBuilder = new ProcessBuilder("java", "-jar", jarPath, "np", "c", String.valueOf(i));
                            Process process = processBuilder.start();
                            processList.add(process);
                            StreamGobbler outputGobbler = new StreamGobbler(process.getInputStream(), "OUTPUT_CLIENT " + String.valueOf(i));
                            outputGobbler.start();
                            StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR_CLIENT " + String.valueOf(i));
                            errorGobbler.start();
                        }
                        for (Process process : processList) {
                            process.waitFor();
                        }
                        mainThread.join();
                    }
                    if ((args[1].equals("c"))) {
                        NamedPipeClient client = new NamedPipeClient();
                        Map<String, Object> configMap = new HashMap<>();
                        configMap.put("id", Integer.parseInt(args[2]));
                        client.init(configMap);
                        client.start();
                    }
                    break;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
