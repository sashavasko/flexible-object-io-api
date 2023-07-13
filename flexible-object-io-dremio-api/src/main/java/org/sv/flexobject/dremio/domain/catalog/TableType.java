package org.sv.flexobject.dremio.domain.catalog;

public enum TableType {
    PHYSICAL_DATASET,
    VIRTUAL_DATASET;

    public Class<? extends Entity> getEntityClass() {
        switch(this){
            case PHYSICAL_DATASET: return Table.class;
            case VIRTUAL_DATASET: return View.class;
            default: throw new UnsupportedOperationException("Unknown dataset class for table type " + this.name());
        }
    }

}
