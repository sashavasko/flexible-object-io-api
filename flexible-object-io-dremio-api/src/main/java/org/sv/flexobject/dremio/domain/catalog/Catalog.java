package org.sv.flexobject.dremio.domain.catalog;

import org.sv.flexobject.dremio.api.CatalogAPI;
import org.sv.flexobject.StreamableImpl;
import org.sv.flexobject.schema.annotations.ValueClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class Catalog extends StreamableImpl {
    @ValueClass(valueClass = CatalogItem.class)
    List<CatalogItem> data = new ArrayList<>();

    public Catalog() {
    }

    public Catalog(List<CatalogItem> data) {
        this.data = data;
    }

    public Optional<CatalogItem> findByPath(String path){
        List<String> parsedPath = Arrays.asList(path.split("\\."));
        return data.stream().filter(i->i.path.equals(parsedPath)).findFirst();
    }

    public <T extends Entity> T traversePath(CatalogAPI catalogApi, List<String> path, Class<? extends Entity> entityClass) {
        Entity entity;
        Optional<CatalogItem> top = findByPath(path.get(0));
        if (!top.isPresent())
            return null;

        Container currentContainer = catalogApi.getById(top.get().id);
        for (int i = 1  ; i < path.size()-1 ; ++i) {
            Optional<Child> child = currentContainer.findChildByName(path.get(i));
            if (!child.isPresent() || child.get().type != CatalogType.CONTAINER)
                return null;

            currentContainer = catalogApi.getById(child.get().id);
        }

        Optional<Child> entityChild = currentContainer.findChildByName(path.get(path.size()-1));
        if (!entityChild.isPresent())
            return null;

        entity = catalogApi.getById(entityChild.get().id);

        return (T)entityClass.cast(entity);
    }
}
