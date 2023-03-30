package org.sv.flexobject.ftp.providers;

import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sv.flexobject.connections.ConnectionProvider;
import org.sv.flexobject.ftp.FTPClient;
import org.sv.flexobject.util.InstanceFactory;

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
        FtpConnectionConf conf = InstanceFactory.get(FtpConnectionConf.class);
        conf.from(connectionProperties);

        if (!login(conf, secret, client)) {
            throw new RuntimeException("Could not login with user " + conf.getUsername() + " for host " + conf.getHost());
        }

        if (conf.hasFtpDirectory())
            if (!client.changeWorkingDirectory(conf.getFtpDirectory()))
                throw new RuntimeException("Failed to change current directory to " + conf.getFtpDirectory());

        logger.info("Created FTP Connection \"" + name + "\" for USER: " + conf.getUsername() + " and host: " + conf.getHost());
        return client;
    }

    private boolean login(FtpConnectionConf conf, Object secret, FTPClient client) throws IOException {
        boolean loginStatus = false;
        FTPClientConfig config = new FTPClientConfig(conf.getSystem());
        client.configure(config);
        client.connect(conf.getHost(), conf.getPort());
        logger.info("Connected to " + conf.getHost() + ".");
        logger.info(client.getReplyString());

        if(!FTPReply.isPositiveCompletion(client.getReplyCode())) {
            client.disconnect();
            logger.error("FTP server refused connection.");
            throw new RuntimeException("FTP server refused connection.");
        }

        if (secret != null) {
            loginStatus = client.login(conf.getUsername(), (String) secret);
        } else if (conf.hasPassword()) {
            loginStatus = client.login(conf.username, conf.getPassword());
        } else {
            logger.info("No password/secret found. Trying anonymous login");
            loginStatus = client.login("anonymous", null);
        }

        client.setDataTimeout(conf.getDataTimeout());
        client.setConnectTimeout(conf.getConnectTimeout());
        client.setRemoteVerificationEnabled(conf.isRemoteVerificationEnabled());
        if (conf.isLocalPassiveMode())
            client.enterLocalPassiveMode();
        client.setUseEPSVwithIPv4(conf.isUseEPSVwithIPv4());
        return loginStatus;
    }

    @Override
    public void close() throws Exception {

    }
}
