package org.sv.flexobject.hadoop.mapreduce.input.mongo.splitters;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.conversions.Bson;
import org.sv.flexobject.hadoop.mapreduce.input.mongo.MongoInputConf;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;
import org.sv.flexobject.properties.Namespace;

public class PaginatedInputConf<SELF extends HadoopPropertiesWrapper> extends MongoInputConf<SELF> {
    public static final String SUBNAMESPACE = "pagination";

    String splitKey;
    Integer minDocs;

    public PaginatedInputConf() {
        super(SUBNAMESPACE);
    }

    public PaginatedInputConf(String child) {
        super(makeMyNamespace(getParentNamespace(PaginatedInputConf.class), SUBNAMESPACE), child);
    }

    public PaginatedInputConf(Namespace parent) {
        super(parent, SUBNAMESPACE);
    }

    public PaginatedInputConf(Namespace parent, String child) {
        super(parent, child);
    }

    @Override
    protected String getSubNamespace() {
        return SUBNAMESPACE;
    }

    @Override
    public SELF setDefaults() {
        super.setDefaults();
        splitterClass = PaginatingSplitter.class;

        splitKey = "_id";
        minDocs = 1000000;
        return (SELF) this;
    }

    public Bson makeQuery(Object start, Object end) {
        return end == null ? Filters.gte(splitKey, start)
               : Filters.and(Filters.gte(splitKey, start),Filters.lt(splitKey, end));
    }

    public Bson makeSplitProjection() {
        return Projections.include(splitKey);
    }

    public Bson makeSort() {
        return Sorts.ascending(splitKey);
    }

    public String getSplitKey() {
        return splitKey;
    }

    public Integer getMinDocs() {
        return minDocs;
    }
}
