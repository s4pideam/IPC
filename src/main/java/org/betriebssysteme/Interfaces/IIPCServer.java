package org.betriebssysteme.Interfaces;

import java.io.DataOutputStream;
import java.util.Map;

import org.betriebssysteme.Enum.EPackage;

public interface IIPCServer {
    void start();

    void stop();

    void send(DataOutputStream dataOutputStream, EPackage epackage, String message);

    void init(Map<String, Object> configMap);

    void updateWordCount(String word, int count);
}
