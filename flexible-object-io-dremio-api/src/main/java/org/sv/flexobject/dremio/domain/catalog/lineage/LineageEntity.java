package org.sv.flexobject.dremio.domain.catalog.lineage;

import org.sv.flexobject.dremio.domain.catalog.CatalogType;
import org.sv.flexobject.StreamableImpl;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ValueType;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class LineageEntity extends StreamableImpl {
    public String id;
    @ValueType(type = DataTypes.string)
    public List<String> path = new ArrayList<>();

    public String tag;
    public CatalogType type;
    public Timestamp createdAt;
}
