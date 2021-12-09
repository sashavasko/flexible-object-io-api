package org.sv.flexobject.ftp.providers;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sv.flexobject.connections.ConnectionProvider;
import org.sv.flexobject.ftp.FTPClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class FtpProvider implements ConnectionProvider, AutoCloseable {
    static Logger logger = LogManager.getLogger(FtpProvider.class);

    @Override
    public Iterable<Class<? extends AutoCloseable>> listConnectionTypes() {
        return Arrays.asList(FTPClient.class);
    }

    @Override
    public AutoCloseable getConnection(String name, Properties connectionProperties, Object secret) throws Exception {
        FTPClient client = new FTPClient();
        String host = connectionProperties.getProperty("host");
        String user = getUser(connectionProperties);
        int port = getPort(connectionProperties);

        boolean loginStatus = login(connectionProperties, secret, client, host, user, port);

        if (loginStatus == false) {
            throw new RuntimeException("Could not login with user " + user + " for host " + host);
        }

        logger.info("Created FTP Connection \"" + name + "\" for USER: " + user + " and host: " + host);
        return client;
    }

    private boolean login(Properties connectionProperties, Object secret, FTPClient client, String host, String user, int port) throws IOException {
        boolean loginStatus = false;

        if (secret != null) {
            client.connect(host, port);
            loginStatus = client.login(user, (String) secret);
        } else if (StringUtils.isNotBlank(connectionProperties.getProperty("password"))) {
            client.connect(host, port);
            loginStatus = client.login(user, connectionProperties.getProperty("password"));
        } else {
            logger.info("No password/secret found. Trying anonymous login");
            client.connect(host, port);
            loginStatus = client.login("anonymous", null);
        }
        return loginStatus;
    }

    private int getPort(Properties connectionProperties) {
        int port = 21;
        if (connectionProperties.getProperty("port") != null) {
            port = Integer.parseInt(connectionProperties.getProperty("port"));
        }
        return port;
    }

    private String getUser(Properties connectionProperties) {
        String user = connectionProperties.getProperty("user");
        if (StringUtils.isBlank(user)) {
            user = connectionProperties.getProperty("username");
        }
        if (StringUtils.isBlank(user)) {
            user = connectionProperties.getProperty("userName");
        }
        return user;
    }

    @Override
    public void close() throws Exception {

    }
}
