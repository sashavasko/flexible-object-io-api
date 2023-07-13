package org.sv.flexobject.dremio.domain.catalog;

public enum UpdateMode {
    PREFETCH,           //: (deprecated) Dremio updates details for all datasets in a source.
    PREFETCH_QUERIED    //: Dremio updates details for previously queried objects in a source.
}
