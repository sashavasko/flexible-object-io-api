package org.sv.flexobject.dremio.domain.catalog;

import org.sv.flexobject.dremio.domain.ApiData;
import org.sv.flexobject.dremio.domain.Permissions;
import org.sv.flexobject.schema.annotations.EnumSetField;
import org.sv.flexobject.schema.annotations.ValueClass;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class Grants extends ApiData {
    public String id; // UUID
    @EnumSetField(enumClass = Permissions.class,emptyValue = "null")
    public EnumSet<Permissions> availablePrivileges = EnumSet.noneOf(Permissions.class);
    @ValueClass(valueClass = Grant.class)
    public List<Grant> grants = new ArrayList<>();
}
