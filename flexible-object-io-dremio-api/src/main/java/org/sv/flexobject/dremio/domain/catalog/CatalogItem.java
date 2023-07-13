package org.sv.flexobject.dremio.domain.catalog;


import org.sv.flexobject.StreamableImpl;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ValueType;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class CatalogItem extends StreamableImpl {
    public String id;
    @ValueType(type = DataTypes.string)
    public List<String> path = new ArrayList<>();
    public String tag;
    public String type;
    public ContainerType containerType;
    public ItemStats stats;
    public Timestamp createdAt;
    public String permissions;

}
