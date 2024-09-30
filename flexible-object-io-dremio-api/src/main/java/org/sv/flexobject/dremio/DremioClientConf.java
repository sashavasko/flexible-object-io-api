package org.sv.flexobject.dremio;


import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.sv.flexobject.connections.ConnectionConf;
import org.sv.flexobject.util.InstanceFactory;

public class DremioClientConf extends ConnectionConf<DremioClientConf> {

    String scheme;
    String hostname;
    int port;
    int flightPort;
    String username;
    String apiPath;

    Class<? extends BufferAllocator> allocatorClass;

    @Override
    public DremioClientConf setDefaults() {
        scheme = "https";
//        hostname = "dremiod02p.d.carfax.us";
        port = 9047;
        flightPort = 32010;
        apiPath = "api/v3";
        allocatorClass = RootAllocator.class;
        return null;
    }

    public String getScheme() {
        return scheme;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public int getFlightPort() {
        return flightPort;
    }

    public String getUsername() {
        return username;
    }

    public String getApiPath() {
        return apiPath;
    }

    public String getUrl() {
        return getUrl(apiPath);
    }

    public String getUrl(String apiPath) {
        if (apiPath == null)
            apiPath = this.apiPath;
        return scheme + "://" + hostname + ":" + port + "/" + apiPath;
    }

    public DremioClientConf setApiPath(String apiPath) {
        this.apiPath = apiPath;
        return this;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setScheme(String scheme) {
        this.scheme = scheme;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public BufferAllocator getAllocator() {
        return InstanceFactory.get(allocatorClass);
    }
}
