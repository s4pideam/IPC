package org.betriebssysteme;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import org.betriebssysteme.Classes.StreamGobbler;
import org.betriebssysteme.IPCVariants.NP.NamedPipeClient;
import org.betriebssysteme.IPCVariants.NP.NamedPipeServer;
import org.betriebssysteme.IPCVariants.PIPE.PipeClient;
import org.betriebssysteme.IPCVariants.PIPE.PipeServer;
import org.betriebssysteme.IPCVariants.TCP.TCPClient;
import org.betriebssysteme.IPCVariants.TCP.TCPServer;
import org.betriebssysteme.IPCVariants.UDS.UDSClient;
import org.betriebssysteme.IPCVariants.UDS.UDSServer;

public class App {

    public static void main(String[] args) throws IOException {
        try {
            String HOST = "localhost";
            int PORT = 42069;
            int CLIENT_NUMBERS;
            int CHUNK_SIZE;
            String FILE_PATH;


            Path execPath = Paths.get(App.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            Path parentPath = execPath.getParent();
            if (parentPath != null && execPath.endsWith("classes")) {
                execPath = parentPath.resolve("ipc.jar");
            }




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
                            ProcessBuilder processBuilder = new ProcessBuilder("java", "-jar", execPath.toString(), "tcp", "c");
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

                        NamedPipeServer server = new NamedPipeServer(FILE_PATH);
                        Map<String, Object> configMap = new HashMap<>();
                        configMap.put("chunkSize", CHUNK_SIZE);
                        configMap.put("clientNumbers", CLIENT_NUMBERS);
                        server.init(configMap);

                        Thread mainThread = new Thread(server::start);
                        mainThread.start();

                        List<Process> processList = new ArrayList<>();
                        for (int i = 0; i < CLIENT_NUMBERS; i++) {
                            ProcessBuilder processBuilder = new ProcessBuilder("java", "-jar", execPath.toString(), "np", "c", String.valueOf(i));
                            Process process = processBuilder.start();
                            processList.add(process);
                            //StreamGobbler outputGobbler = new StreamGobbler(process.getInputStream(), "OUTPUT_CLIENT " + String.valueOf(i));
                            //outputGobbler.start();
                            //StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR_CLIENT " + String.valueOf(i));
                            //errorGobbler.start();
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
                case "pipe": {
                    if (args[1].equals("s")) {

                        CLIENT_NUMBERS = Integer.parseInt(args[2]);
                        CHUNK_SIZE = Integer.parseInt(args[3]);
                        FILE_PATH = args[4];

                        PipeServer server = new PipeServer(FILE_PATH);
                        Map<String, Object> configMap = new HashMap<>();
                        configMap.put("chunkSize", CHUNK_SIZE);
                        configMap.put("clientNumbers", CLIENT_NUMBERS);

                        List<Process> processList = new ArrayList<>();
                        ProcessBuilder processBuilder = new ProcessBuilder("java", "-jar", execPath.toString(), "pipe", "c");
                        for (int i = 0; i < CLIENT_NUMBERS; i++) {
                            Process process = processBuilder.start();
                            processList.add(process);
                            //StreamGobbler outputGobbler = new StreamGobbler(process.getInputStream(), "OUTPUT_CLIENT " + String.valueOf(i));
                            //outputGobbler.start();
                            //StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR_CLIENT " + String.valueOf(i));
                            //errorGobbler.start();
                        }

                        server.setProcessList(processList);
                        server.init(configMap);

                        server.start();


                    }
                    if ((args[1].equals("c"))) {
                        PipeClient client = new PipeClient();
                        Map<String, Object> configMap = new HashMap<>();
                        client.init(configMap);
                        client.start();
                    }
                    break;
                }
                case "uds": {
                    if(args[1].equals("s")){
                        CLIENT_NUMBERS = Integer.parseInt(args[2]);
                        CHUNK_SIZE = Integer.parseInt(args[3]);
                        FILE_PATH = args[4];
                        UDSServer server = new UDSServer(FILE_PATH);
                        Map<String, Object> configMap = new HashMap<>();
                        configMap.put("chunkSize", CHUNK_SIZE);
                        configMap.put("clientNumbers", CLIENT_NUMBERS);
                        server.init(configMap);

                        Thread mainThread = new Thread(server::start);
                        mainThread.start();

                        List<Process> processList = new ArrayList<>();
                        for (int i = 0; i < CLIENT_NUMBERS; i++) {
                            ProcessBuilder processBuilder = new ProcessBuilder("java", "-jar", execPath.toString(), "uds", "c");
                            Process process = processBuilder.start();
                            processList.add(process);
                            //StreamGobbler outputGobbler = new StreamGobbler(process.getInputStream(), "OUTPUT_CLIENT " + String.valueOf(i));
                            //outputGobbler.start();
                            //StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR_CLIENT " + String.valueOf(i));
                            //errorGobbler.start();
                        }
                        for (Process process : processList) {
                            process.waitFor();
                        }
                        mainThread.join();
                    }
                    if ((args[1].equals("c"))) {
                        System.out.println("start");
                        UDSClient client = new UDSClient();
                        Map<String, Object> configMap = new HashMap<>();
                        configMap.put("host", HOST);
                        configMap.put("port", PORT);
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
