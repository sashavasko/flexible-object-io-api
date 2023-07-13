package org.sv.flexobject.dremio.domain.catalog.config;

import org.sv.flexobject.dremio.domain.catalog.CtasFormat;

public class HdfsConf extends SourceConf<HdfsConf>{
    public Boolean enableImpersonation;
    public String rootPath;
    public ShortCircuitFlags shortCircuitFlag;
    public Boolean enableAsync;
    public Boolean isCachingEnabled;
    public Integer maxCacheSpacePct;
    public CtasFormat defaultCtasFormat;// = "ICEBERG";
    public Boolean isPartitionInferenceEnabled;// = false;
    public String impersonationUserDelegationMode;// = "AS_IS";
    public Boolean vdsAccessDelegationEnabled;// = true;

    public HdfsConf() {
        port = 8020;
        addProperty("dfs.client.socket-timeout","120000");
    }

    @Override
    public String getType() {
        return "HDFS";
    }
}
