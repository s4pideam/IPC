package org.betriebssysteme.Interfaces;

import org.betriebssysteme.Enum.EPackage;

import java.io.DataOutputStream;
import java.util.Map;



public interface IIPCClient {
    void start();
    void disconnect();
    void send(DataOutputStream dataOutputStream, EPackage epackage, String message);
    void init(Map<String, Object> configMap);
}
