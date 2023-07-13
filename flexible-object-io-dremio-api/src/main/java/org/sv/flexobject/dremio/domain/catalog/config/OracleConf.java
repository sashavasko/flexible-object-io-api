package org.sv.flexobject.dremio.domain.catalog.config;

public class OracleConf extends SourceConf<OracleConf> {
    public String instance;
    public boolean useTimezoneAsRegion = true;
    public boolean includeSynonyms = false;
    public boolean useLdap = false;
    public NativeEncryptionType nativeEncryption = NativeEncryptionType.ACCEPTED;
    public boolean useKerberos = false;
    public int maxIdleConns = 8;
    public int idleTimeSec = 60;
    public boolean mapDateToTimestamp = true;
    public int queryTimeoutSec = 0;

    public OracleConf() {
        addProperty("connect_timeout", "0");
        port = 1521;
    }

    @Override
    public String getType() {
        return "ORACLE";
    }
}
