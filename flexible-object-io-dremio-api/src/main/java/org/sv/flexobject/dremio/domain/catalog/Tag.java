package org.sv.flexobject.dremio.domain.catalog;

import org.sv.flexobject.dremio.domain.ApiData;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ValueType;

import java.util.ArrayList;
import java.util.List;

public class Tag extends ApiData {
    @ValueType(type = DataTypes.string)
    public List<String> tags = new ArrayList<>();
    public String version;
}
