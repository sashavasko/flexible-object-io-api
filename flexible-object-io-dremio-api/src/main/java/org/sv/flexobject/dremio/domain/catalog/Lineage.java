package org.sv.flexobject.dremio.domain.catalog;

import org.sv.flexobject.dremio.domain.ApiData;
import org.sv.flexobject.dremio.domain.catalog.lineage.Dataset;
import org.sv.flexobject.dremio.domain.catalog.lineage.Source;
import org.sv.flexobject.schema.annotations.ValueClass;

import java.util.ArrayList;
import java.util.List;

public class Lineage extends ApiData {
    @ValueClass(valueClass = org.sv.flexobject.dremio.domain.catalog.lineage.Source.class)
    public List<Source> sources = new ArrayList<>();

    @ValueClass(valueClass = org.sv.flexobject.dremio.domain.catalog.lineage.Dataset.class)
    public List<org.sv.flexobject.dremio.domain.catalog.lineage.Dataset> parents = new ArrayList<>();

    @ValueClass(valueClass = org.sv.flexobject.dremio.domain.catalog.lineage.Dataset.class)
    public List<Dataset> children = new ArrayList<>();


}
