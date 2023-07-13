package org.sv.flexobject.dremio.domain.catalog;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class Space extends Container {
    public Timestamp createdAt;

    public Space() {
        entityType = EntityType.space;
    }

    @Override
    public List<String> getPath() {
        return Arrays.asList(name);
    }
}
