package org.sv.flexobject.dremio.domain.catalog;

import org.sv.flexobject.dremio.api.DremioApiException;
import org.sv.flexobject.dremio.domain.catalog.config.HdfsConf;
import org.sv.flexobject.dremio.domain.catalog.config.OracleConf;
import org.sv.flexobject.dremio.domain.catalog.config.SourceConf;

public enum SourceType {
    ADL,
    ADX,
    AMAZONELASTIC,
    AWSGLUE,
    AZURE_STORAGE,
    DB2,
    DREMIOTODREMIO,
    ELASTIC,
    GCS,
    HDFS,
    HIVE,
    HIVE3,
    MONGO,
    MSSQL,
    MYSQL,
    NAS,
    NESSIE,
    ORACLE,
    POSTGRES,
    REDSHIFT,
    S3,
    SNOWFLAKE,
    SYNAPSE,
    TERADATA;

    public Class<? extends SourceConf> getConfClass() {
        switch(this){
            case HDFS: return HdfsConf.class;
            case ORACLE: return OracleConf.class;
            default: throw new DremioApiException("Unimplemented source type " + this.name());
        }
    }
}
