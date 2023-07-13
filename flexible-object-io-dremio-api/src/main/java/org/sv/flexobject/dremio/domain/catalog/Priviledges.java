package org.sv.flexobject.dremio.domain.catalog;

import org.sv.flexobject.dremio.domain.ApiData;
import org.sv.flexobject.dremio.domain.Permissions;
import org.sv.flexobject.schema.annotations.ValueClass;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;

public class Priviledges extends ApiData {
    @ValueClass(valueClass = AvailablePriviledges.class)
    public List<AvailablePriviledges> availablePrivileges = new ArrayList<>();

    public EnumSet<Permissions> getPermissions(GrantType grantType){
        Optional<AvailablePriviledges> availablePriviledges = availablePrivileges.stream()
                .filter(ap->ap.grantType == grantType)
                .findFirst();

        return availablePriviledges.isPresent() ?
                availablePriviledges
                        .get()
                        .privileges
                : EnumSet.noneOf(Permissions.class);
    }
}
