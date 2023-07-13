package org.sv.flexobject.dremio.domain.catalog.config;


import org.sv.flexobject.dremio.domain.catalog.CtasFormat;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ValueType;

import java.util.List;

public class AmazonS3Conf extends SourceConf<AmazonS3Conf>{
    public String assumedRoleARN;
    public Boolean secure;
    public String rootPath;
    public Boolean enableAsync;
    public Boolean compatibilityMode;
    public Boolean isCachingEnabled;
    public Integer maxCacheSpacePct;
    public Boolean requesterPays;
    public Boolean enableFileStatusCheck;
    public CtasFormat defaultCtasFormat;
    public Boolean isPartitionInferenceEnabled;

    @ValueType(type = DataTypes.string)
    public List<String> externalBucketList;
    @ValueType(type = DataTypes.string)
    public List<String> whitelistedBuckets;

    @Override
    public String getType() {
        return "S3";
    }
}
