package org.sv.flexobject.hadoop.mapreduce.drivers;

import com.sun.xml.internal.messaging.saaj.packaging.mime.MessagingException;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.io.IOException;

abstract public class FTPDriver extends Configured implements Tool{
    Logger logger = Logger.getLogger(FTPDriver.class);

    protected String ftpHost;
    protected String ftpUser;
    protected String ftpPass;
    protected String ftpDirectory;

    @Override
    public int run(String[] args) throws Exception {
        FTPClient ftp = new FTPClient();
        try {
            ftp = getConnectedFtpClient(ftp);

            logger.info(ftp.getStatus());

            if (ftpDirectory != null && !ftp.changeWorkingDirectory(ftpDirectory))
                throw new RuntimeException("Failed to change current directory to " + ftpDirectory);

            ftpFiles(ftp);
        }catch (Exception e ){
            logger.error("Connection to " + ftpHost, e);
            throw new RuntimeException(e);
        }
        finally{
            disconnect(ftp);
        }
        return 0;
    }

    public FTPClient getConnectedFtpClient(FTPClient ftp) throws IOException {
        FTPClientConfig config = new FTPClientConfig(FTPClientConfig.SYST_UNIX);
        ftp.configure(config);
        ftp.connect(ftpHost);
        logger.info("Connected to " + ftpHost + ".");
        logger.info(ftp.getReplyString());

        if(!FTPReply.isPositiveCompletion(ftp.getReplyCode())) {
            ftp.disconnect();
            logger.error("FTP server refused connection.");
            throw new RuntimeException("FTP server refused connection.");
        }

        ftp.login(ftpUser, ftpPass);
        logger.info("Authenticated user " + ftpUser);
        ftp.setDataTimeout(600000);
        ftp.setConnectTimeout(600000);
        ftp.setRemoteVerificationEnabled(false);
        ftp.enterLocalPassiveMode();
        ftp.setUseEPSVwithIPv4(true);
        return ftp;
    }

    public void disconnect(FTPClient ftp) {

        if(ftp.isConnected()) {
            try {
                ftp.logout();
                ftp.disconnect();
                logger.info("Disconnected from " + ftpHost);
            } catch(IOException ioe) {
                // do nothing
            }
        }
    }


    abstract public void ftpFiles(FTPClient ftp) throws IOException, MessagingException;
}
