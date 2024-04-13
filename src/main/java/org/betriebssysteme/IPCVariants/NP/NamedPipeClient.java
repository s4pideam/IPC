package org.betriebssysteme.IPCVariants.NP;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Map;

import org.betriebssysteme.Classes.OutputStreamClient;
import org.betriebssysteme.IPCVariants.TCP.TCPClient;
import org.betriebssysteme.Utils.Utils;

public class NamedPipeClient extends OutputStreamClient {

    public NamedPipeClient()  {
    }
    @Override
    public void init(Map<String, Object> configMap) {
        int id = (int)configMap.get("id");
        Path serverToClient = FileSystems.getDefault().getPath("./np/server_to_client" + id);
        Path clientToServer = FileSystems.getDefault().getPath("./np/client_to_server" + id);

        Utils.createNamedPipe(serverToClient);
        Utils.createNamedPipe(clientToServer);

        try {
            this.outputStream = new DataOutputStream(new FileOutputStream(clientToServer.toString()));
            this.inputStream = new DataInputStream(new FileInputStream(serverToClient.toString()));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

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
