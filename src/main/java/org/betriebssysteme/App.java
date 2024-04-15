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
import org.betriebssysteme.IPCVariants.ZMQ.ZMQClient;
import org.betriebssysteme.IPCVariants.ZMQ.ZMQServer;
import org.betriebssysteme.Plain.Single.Single;
import org.betriebssysteme.Plain.Threaded.ThreadServer;

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
            long startTime = System.nanoTime();
            switch (args[0]) {
                case "single": {
                    CHUNK_SIZE = Integer.parseInt(args[1]);
                    FILE_PATH = args[2];
                    Single single = new Single(FILE_PATH,CHUNK_SIZE);
                    single.count();
                    double runtimeInSeconds = (System.nanoTime() - startTime) / 1e9;
                    System.out.println("Runtime: " + runtimeInSeconds + " s");
                    break;
                }
                case "threads": {
                    CLIENT_NUMBERS = Integer.parseInt(args[1]);
                    CHUNK_SIZE = Integer.parseInt(args[2]);
                    FILE_PATH = args[3];

                    ThreadServer server = new ThreadServer(FILE_PATH);
                    Map<String, Object> configMap = new HashMap<>();
                    configMap.put("chunkSize", CHUNK_SIZE);
                    configMap.put("clientNumbers", CLIENT_NUMBERS);
                    server.init(configMap);

                    server.start();
                    double runtimeInSeconds = (System.nanoTime() - startTime) / 1e9;
                    System.out.println("Runtime: " + runtimeInSeconds + " s");
                    break;
                }
                case "tcp": {
                    if ((args[1].equals("c"))) {
                        System.out.println("start");
                        TCPClient client = new TCPClient();
                        Map<String, Object> configMap = new HashMap<>();
                        configMap.put("host", HOST);
                        configMap.put("port", PORT);
                        client.init(configMap);
                        client.start();
                    }else{
                        CLIENT_NUMBERS = Integer.parseInt(args[1]);
                        CHUNK_SIZE = Integer.parseInt(args[2]);
                        FILE_PATH = args[3];

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
                        double runtimeInSeconds = (System.nanoTime() - startTime) / 1e9;
                        System.out.println("Runtime: " + runtimeInSeconds + " s");
                    }
                    break;
                }
                case "np": {

                    if ((args[1].equals("c"))) {
                        NamedPipeClient client = new NamedPipeClient();
                        Map<String, Object> configMap = new HashMap<>();
                        configMap.put("id", Integer.parseInt(args[2]));
                        client.init(configMap);
                        client.start();
                    }else{
                        CLIENT_NUMBERS = Integer.parseInt(args[1]);
                        CHUNK_SIZE = Integer.parseInt(args[2]);
                        FILE_PATH = args[3];

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
                        double runtimeInSeconds = (System.nanoTime() - startTime) / 1e9;
                        System.out.println("Runtime: " + runtimeInSeconds + " s");
                    }
                    break;
                }
                case "pipes": {
                    if ((args[1].equals("c"))) {
                        PipeClient client = new PipeClient();
                        Map<String, Object> configMap = new HashMap<>();
                        client.init(configMap);
                        client.start();
                    }else{
                        CLIENT_NUMBERS = Integer.parseInt(args[1]);
                        CHUNK_SIZE = Integer.parseInt(args[2]);
                        FILE_PATH = args[3];

                        PipeServer server = new PipeServer(FILE_PATH);
                        Map<String, Object> configMap = new HashMap<>();
                        configMap.put("chunkSize", CHUNK_SIZE);
                        configMap.put("clientNumbers", CLIENT_NUMBERS);

                        List<Process> processList = new ArrayList<>();
                        ProcessBuilder processBuilder = new ProcessBuilder("java", "-jar", execPath.toString(), "pipes", "c");
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
                        double runtimeInSeconds = (System.nanoTime() - startTime) / 1e9;
                        System.out.println("Runtime: " + runtimeInSeconds + " s");
                    }
                    break;
                }
                case "uds": {
                    if ((args[1].equals("c"))) {
                        System.out.println("start");
                        UDSClient client = new UDSClient();
                        Map<String, Object> configMap = new HashMap<>();
                        configMap.put("host", HOST);
                        configMap.put("port", PORT);
                        client.init(configMap);
                        client.start();
                    }else{
                        CLIENT_NUMBERS = Integer.parseInt(args[1]);
                        CHUNK_SIZE = Integer.parseInt(args[2]);
                        FILE_PATH = args[3];
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
                        double runtimeInSeconds = (System.nanoTime() - startTime) / 1e9;
                        System.out.println("Runtime: " + runtimeInSeconds + " s");
                    }
                    break;
                }
                case "zmq": {
                    if (args[1].equals("c")) {
                        // Client mode
                        ZMQClient client = new ZMQClient();
                        Map<String, Object> configMap = new HashMap<>();
                        configMap.put("host", HOST);
                        configMap.put("port", PORT);
                        client.init(configMap);
                        client.start();
                    } else {
                        // Server mode
                        CLIENT_NUMBERS = Integer.parseInt(args[1]);
                        CHUNK_SIZE = Integer.parseInt(args[2]);
                        FILE_PATH = args[3];

                        ZMQServer server = new ZMQServer(FILE_PATH);
                        Map<String, Object> configMap = new HashMap<>();
                        configMap.put("port", PORT);
                        configMap.put("chunkSize", CHUNK_SIZE);
                        configMap.put("clientNumbers", CLIENT_NUMBERS);
                        server.init(configMap);

                        Thread mainThread = new Thread(server::start);
                        mainThread.start();

                        List<Process> processList = new ArrayList<>();
                        for (int i = 0; i < CLIENT_NUMBERS; i++) {
                            ProcessBuilder processBuilder = new ProcessBuilder("java", "-jar", execPath.toString(), "zmq", "c");
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
                        double runtimeInSeconds = (System.nanoTime() - startTime) / 1e9;
                        System.out.println("Runtime: " + runtimeInSeconds + " s");
                    }
                    break;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
