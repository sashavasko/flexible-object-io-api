package org.sv.flexobject.ftp.providers;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class FtpConnectionConfTest {
    @Test
    public void listSettings() {
        FtpConnectionConf conf = new FtpConnectionConf();
        List<String> expectedSettings = Arrays.asList(
                "host",
                "username",
                "password",
                "port",
                "dataTimeout",
                "connectTimeout",
                "remoteVerificationEnabled",
                "localPassiveMode",
                "useEPSVwithIPv4",
                "system",
                "ftpDirectory");
        List<String> actualSettings = conf.listSettings();

        assertEquals(expectedSettings, actualSettings);

    }
}