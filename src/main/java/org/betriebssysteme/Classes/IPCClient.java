package org.betriebssysteme.Classes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Map;
import org.betriebssysteme.Enum.EPackage;
import org.betriebssysteme.Interfaces.IIPCClient;

public class IPCClient implements IIPCClient{
    protected DataOutputStream outputStream;
    protected DataInputStream inputStream;
    protected boolean connected = true;

    @Override
    public void start() {
        throw new UnsupportedOperationException("Unimplemented method 'connect'");
    }

    @Override
    public void disconnect() {
        throw new UnsupportedOperationException("Unimplemented method 'disconnect'");
    }

    @Override
    public void init(Map<String, Object> configMap) {

        throw new UnsupportedOperationException("Unimplemented method 'init'");
    }

    @Override
    public void send(DataOutputStream dataOutputStream, EPackage epackage, String message) {
        throw new UnsupportedOperationException("Unimplemented method 'init'");
    }

}
