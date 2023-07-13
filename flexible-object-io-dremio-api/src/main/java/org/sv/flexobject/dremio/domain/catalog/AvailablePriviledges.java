package org.sv.flexobject.dremio.domain.catalog;

import org.sv.flexobject.dremio.domain.ApiData;
import org.sv.flexobject.dremio.domain.Permissions;
import org.sv.flexobject.schema.annotations.EnumSetField;

import java.util.EnumSet;

public class AvailablePriviledges extends ApiData {
    public GrantType grantType;
    @EnumSetField(enumClass = Permissions.class,emptyValue = "null")
    public EnumSet<Permissions> privileges = EnumSet.noneOf(Permissions.class);
}
