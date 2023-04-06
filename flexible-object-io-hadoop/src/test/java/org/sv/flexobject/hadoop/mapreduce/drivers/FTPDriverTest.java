package org.sv.flexobject.hadoop.mapreduce.drivers;

import org.junit.Before;
import org.junit.Test;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.connections.PropertiesProvider;
import org.sv.flexobject.ftp.FTPClient;
import org.sv.flexobject.ftp.providers.FtpProvider;

import javax.mail.MessagingException;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;

public class FTPDriverTest {

    public static class TestFtpDriver extends FTPDriver{
        FTPClient filesTarget;

        public TestFtpDriver(String ftpConnectionName) {
            super(ftpConnectionName);
        }

        @Override
        public void ftpFiles(FTPClient ftp) throws IOException, MessagingException {
            filesTarget = ftp;
        }
    }

    public static final String CONNECTION_NAME = "testFtpConnection";
    TestFtpDriver driver = new TestFtpDriver(CONNECTION_NAME);
    private FakeFtpServer fakeFtpServer;

    @Before
    public void setUp() throws Exception {
        fakeFtpServer = new FakeFtpServer();
        fakeFtpServer.addUserAccount(new UserAccount("user", "password", "/data"));

        FileSystem fileSystem = new UnixFakeFileSystem();
        fileSystem.add(new DirectoryEntry("/data"));
        fileSystem.add(new FileEntry("/data/foobar.txt", "abcdef 1234567890"));
        fakeFtpServer.setFileSystem(fileSystem);
        fakeFtpServer.setServerControlPort(0);

        fakeFtpServer.start();
        PropertiesProvider testFtpPropertiesProvider = new PropertiesProvider() {
            @Override
            public Properties getProperties(String connectionName, ConnectionManager.DeploymentLevel deploymentLevel, String environment) {
                if (CONNECTION_NAME.equals(connectionName)) {
                    Properties props = new Properties();
                    props.put("host", "localhost");
                    props.put("port", fakeFtpServer.getServerControlPort());
                    props.put("username", "user");
                    props.put("password", "password");
                    return props;
                }
                return null;
            }
        };
        ConnectionManager.getInstance().registerPropertiesProvider(testFtpPropertiesProvider);
        ConnectionManager.getInstance().registerProvider(new FtpProvider());
    }

    @Test
    public void run() throws Exception {
        driver.run(new String[]{});
        assertNotNull(driver.filesTarget);
    }

}