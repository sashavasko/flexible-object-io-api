package org.sv.flexobject.dremio.domain.catalog;

public enum EntityType {
    source,
    home,
    space,
    folder,
    file,
    dataset;

    public Class<? extends Entity> getEntityClass() {
        switch(this){
            case source:
            case home: return Source.class;
            case space: return Space.class;
            case folder: return Folder.class;
            case file: return File.class;
            case dataset: return Dataset.class;
            default: throw new UnsupportedOperationException("Unknown class for entity type " + this.name());
        }
    }
}
