package org.sv.flexobject.dremio.domain.catalog;

import org.sv.flexobject.dremio.api.DremioApiException;
import org.sv.flexobject.dremio.domain.catalog.config.SourceConf;
import com.fasterxml.jackson.databind.JsonNode;
import org.sv.flexobject.util.InstanceFactory;

import java.util.*;

public class Source extends Container {
    public JsonNode config;    // The config object’s parameters vary for different source types.
                        // https://docs.dremio.com/software/rest-api/catalog/container-source-config/
    /***
     *  Unique identifier of the version of the source that you want to update.
     *  Dremio uses the tag to ensure that you are requesting to update the most
     *  recent version of the source.
     *  Example:   T0/Zr1FOY3A=
     */
    public SourceType type;
    public MetadataPolicy metadataPolicy;

    public Long accelerationGracePeriodMs;
    public Long accelerationRefreshPeriodMs;
    public Boolean accelerationNeverExpire;
    public Boolean accelerationNeverRefresh;
    public Boolean allowCrossSourceSelection;
    public Boolean disableMetadataValidityCheck;

    public Boolean checkTableAuthorizer;

    public Source() {
        entityType = EntityType.source;
    }

    public <T extends SourceConf> T getConfig() {
        Class <? extends SourceConf> confClass = type.getConfClass();
        SourceConf conf = InstanceFactory.get(confClass);
        try {
            conf.fromJson(config);
        } catch (Exception e) {
            throw new DremioApiException("Failed to parse config of class " + confClass + " for Source " + id, e);
        }
        return (T) confClass.cast(conf);
    }

    public void setConfig(SourceConf conf) {
        try {
            config = conf.toJson();
        } catch (Exception e) {
            throw new DremioApiException ("Failed to convert config to Json for Source " + id, e);
        }
    }

    @Override
    public List<String> getPath() {
        return Arrays.asList(name);
    }
}
