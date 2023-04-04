package org.sv.flexobject.ftp.providers;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.sv.flexobject.connections.ConnectionConf;

import java.util.Map;

public class FtpConnectionConf extends ConnectionConf<FtpConnectionConf> {

    String host;
    String username;
    String password;
    int port;

    int dataTimeout;//(600000);
    int connectTimeout;//(600000);
    boolean remoteVerificationEnabled;//(false);
    boolean localPassiveMode; // true
    boolean useEPSVwithIPv4;//(true);
    String system; // UNIX
    protected String ftpDirectory;


    public FtpConnectionConf() {
    }

    public FtpConnectionConf(String host, int port, String username) {
        this.host = host;
        this.port = port;
        this.username = username;
    }

    @Override
    public FtpConnectionConf setDefaults() {
        port = 21;
        dataTimeout = 600000;
        connectTimeout = 600000;
        remoteVerificationEnabled = false;
        localPassiveMode = true;
        useEPSVwithIPv4 = true;
        system = FTPClientConfig.SYST_UNIX;
        return this;
    }

    public String getHost() {
        return host;
    }

    public String getUsername() {
        return username;
    }
    public boolean hasUsername() {
        return StringUtils.isNotBlank(username);
    }

    public boolean hasFtpDirectory() {
        return StringUtils.isNotBlank(ftpDirectory);
    }

    public String getFtpDirectory() {
        return ftpDirectory;
    }

    public boolean hasPassword() {
        return StringUtils.isNotBlank(password);
    }

    public String getPassword() {
        return password;
    }

    public int getPort() {
        return port;
    }

    public int getDataTimeout() {
        return dataTimeout;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public boolean isRemoteVerificationEnabled() {
        return remoteVerificationEnabled;
    }

    public boolean isLocalPassiveMode() {
        return localPassiveMode;
    }

    public boolean isUseEPSVwithIPv4() {
        return useEPSVwithIPv4;
    }

    public String getSystem() {
        return system;
    }

    @Override
    public FtpConnectionConf from(Map source) throws Exception {
        super.from(source);
        if (!hasUsername())
            username = (String) source.get("user");
        if (!hasUsername())
            username = (String) source.get("userName");

        return this;
    }
}
