package org.sv.flexobject.ftp.providers;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;
import org.sv.flexobject.ftp.FTPClient;

import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class FtpProviderTest {


    private FakeFtpServer fakeFtpServer;
    private int port = 9187;

    private static final String HOME_DIR = "/";
    private static final String FILE = "/dir/sample.txt";
    private static final String CONTENTS = "abcdef 1234567890";

    @BeforeEach
    public void setUp() {
        fakeFtpServer = new FakeFtpServer();
        fakeFtpServer.setServerControlPort(port);

        FileSystem fileSystem = new UnixFakeFileSystem();
        fileSystem.add(new FileEntry(FILE, CONTENTS));
        fakeFtpServer.setFileSystem(fileSystem);

        UserAccount userAccount = new UserAccount("userName", "pAssword", HOME_DIR);
        UserAccount userAccount2 = new UserAccount("anonymous", "", HOME_DIR);
        userAccount2.setPasswordRequiredForLogin(false);
        fakeFtpServer.addUserAccount(userAccount);
        fakeFtpServer.addUserAccount(userAccount2);

        fakeFtpServer.start();
    }

    @AfterEach
    public void tearDown() {
        fakeFtpServer.stop();
    }

//    @Test
//    public void basic() throws IOException {
//        FTPClient ftpClient = new FTPClient();
//        ftpClient.connect("localhost", port);
//        ftpClient.login("userName", "pAssword");
//        System.out.println(ftpClient.isConnected());
//        System.out.println(ftpClient.cwd("/dir"));
//        System.out.println(ftpClient.getStatus());
//    }

    @Test
    public void getConnection() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("host", "localhost");
        properties.setProperty("user", "userName");
        properties.setProperty("port", String.valueOf(port));
        properties.setProperty("password", "pAssword");
        FtpProvider provider = new FtpProvider();

        try (FTPClient client = (FTPClient) provider.getConnection("foo", properties, null)) {
            assertNotNull(client);
            assertTrue(client.isConnected());
        }
    }

    @Test
    public void getConnectionUsingSecret() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("host", "localhost");
        properties.setProperty("userName", "userName");
        properties.setProperty("port", String.valueOf(port));
        FtpProvider provider = new FtpProvider();

        try (FTPClient client = (FTPClient) provider.getConnection("foo", properties, "pAssword")) {
            assertNotNull(client);
            assertTrue(client.isConnected());
        }
    }

    @Test
    public void getConnectionUsingAnonymous() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("host", "localhost");
        properties.setProperty("username", "anonymous");
        properties.setProperty("port", String.valueOf(port));
        FtpProvider provider = new FtpProvider();

        try (FTPClient client = (FTPClient) provider.getConnection("foo", properties, null)) {
            assertNotNull(client);
            assertTrue(client.isConnected());
            assertEquals(Arrays.asList(FTPClient.class), provider.listConnectionTypes());
        }
    }

    @Test
    public void throwsErrorOnNoConnection() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("host", "localhost");
        properties.setProperty("user", "badUser");
        properties.setProperty("password", "badPassword");
        properties.setProperty("port", String.valueOf(port));
        FtpProvider provider = new FtpProvider();

        assertThrows(RuntimeException.class, ()->provider.getConnection("foo", properties, null));
    }


}