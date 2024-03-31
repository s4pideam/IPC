package org.betriebssysteme.Interfaces;

import java.util.Map;



public interface IIPCClient {
    void connect();
    void disconnect();
    void init(Map<String, Object> configMap);
}
