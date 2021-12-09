package org.sv.flexobject.ftp;

public class FTPClient extends org.apache.commons.net.ftp.FTPClient implements AutoCloseable {

    @Override
    public void close() throws Exception {
        try {
            disconnect();
        } catch (Exception e) {

        }
    }
}
