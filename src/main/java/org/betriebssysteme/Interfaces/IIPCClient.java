package org.betriebssysteme.Interfaces;

import java.util.Map;



public interface IIPCClient {
    void start();
    void disconnect();
    void init(Map<String, Object> configMap);
}
