package org.sv.flexobject.hadoop.mapreduce.drivers;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.ftp.FTPClient;

import javax.mail.MessagingException;
import java.io.IOException;

abstract public class FTPDriver extends Configured implements Tool{
    static public Logger logger = Logger.getLogger(FTPDriver.class);

    protected String ftpConnectionName;

    FTPClient ftp;

    public void setFtpConnectionName(String ftpConnectionName) {
        this.ftpConnectionName = ftpConnectionName;
    }

    public FTPDriver() {
    }

    public FTPDriver(String ftpConnectionName) {
        this.ftpConnectionName = ftpConnectionName;
    }

    @Override
    public int run(String[] args) throws Exception {
        ftp = (FTPClient) ConnectionManager.getConnection(FTPClient.class, ftpConnectionName);
        try {
            logger.info(ftp.getStatus());

            ftpFiles(ftp);
        }catch (Exception e ){
            logger.error("Failed to connect to ftp " + ftpConnectionName, e);
            throw new RuntimeException(e);
        }finally {
            disconnect();
        }
        return 0;
    }

    public void disconnect() {

        if(ftp != null && ftp.isConnected()) {
            try {
                ftp.logout();
                ftp.disconnect();
                ftp = null;
                logger.info("Disconnected from ftp " + ftpConnectionName);
            } catch(IOException ioe) {
                // do nothing
            }
        }
    }

    public void reconnect(boolean force) throws Exception {
        if (!ftp.isConnected() || force) {
            logger.info("FTP isConnected:" + ftp.isConnected());
            disconnect();
            logger.info("FTP has been disconnected for retry.");
            ftp = (FTPClient) ConnectionManager.getConnection(FTPClient.class, ftpConnectionName);;
            logger.info("Got new FTP Connection. Ftp.isConnected(): " + ftp.isConnected());
            logger.info("FTP Status after reconnection: " + ftp.getStatus());
            logger.info("FTP working directory: " + ftp.printWorkingDirectory());
        }
    }

    abstract public void ftpFiles(FTPClient ftp) throws IOException, MessagingException;
}
