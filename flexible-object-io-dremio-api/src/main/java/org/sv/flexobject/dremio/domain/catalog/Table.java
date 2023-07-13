package org.sv.flexobject.dremio.domain.catalog;

import org.sv.flexobject.dremio.api.CatalogAPI;
import org.sv.flexobject.dremio.domain.AccelerationRefreshPolicy;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class Table extends Dataset{

    public AccelerationRefreshPolicy accelerationRefreshPolicy;
    public Format format;
    public Boolean approximateStatisticsAllowed; // If true, COUNT DISTINCT queries run on the table return approximate results. Otherwise, false.

    public Table() {
        super();
        type = TableType.PHYSICAL_DATASET;
    }

    public static class Builder {
        public CatalogAPI catalogAPI;
        public List<Container> path = new ArrayList<>();
        public String datasetName;
        public Entity dataset;

        private Builder(CatalogAPI catalogAPI) {
            this.catalogAPI = catalogAPI;
        }

        public Builder fullPath (String ... path) throws FileNotFoundException {
            return fullPath(Arrays.asList(path));
        }

        public Builder fullPath (List<String> path) throws FileNotFoundException {
            startAt(path.get(0));
            for (int i = 1 ; i < path.size()-1 ; ++i)
                subFolder(path.get(i));
            dataset(path.get(path.size()-1));
            return this;
        }
        public Builder startAt (String containerName) throws FileNotFoundException {
            path.clear();
            Catalog catalog = catalogAPI.catalog();
            Optional<CatalogItem> top = catalog.findByPath(containerName);
            if (!top.isPresent())
                throw new FileNotFoundException("Source, Space or Home " + containerName + " not found");
            Container startingContainer = catalogAPI.getById(top.get().id);
            path.add(startingContainer);
            return this;
        }

        protected Child findChildAtCurrentPath(String name) throws FileNotFoundException {
            Container previous = path.get(path.size()-1);
            Optional<Child> child = previous.findChildByName(name);
            if (!child.isPresent())
                throw new FileNotFoundException("Entity " + name + " not found");
            return child.get();
        }
        public Builder subFolder(String containerName) throws FileNotFoundException {
            Child child = findChildAtCurrentPath(containerName);
            if (child.type != CatalogType.CONTAINER)
                throw new FileNotFoundException("Entity " + containerName + " is not a container");
            Container folder = catalogAPI.getById(child.id);
            path.add(folder);
            return this;
        }

        public Builder dataset(String name) throws FileNotFoundException {
            Child child = findChildAtCurrentPath(name);
            dataset = catalogAPI.getById(child.id);
            datasetName = name;
            return this;
        }

        public Dataset get(){
            if (dataset.entityType == EntityType.dataset) {
                return (Dataset) dataset;
            }

            return null;
        }
        public Table format(AccessControlList accessControlList, Format format, boolean forceReformat) throws FileNotFoundException {
            if (dataset.entityType == EntityType.dataset){
                if (!forceReformat)
                    return (Table) dataset;

                catalogAPI.delete(dataset);
                // dataset IDs may have changed - need to refresh entire path!
                fullPath(dataset.getPath());
            }
            String datasetId;
            try {
                datasetId = URLEncoder.encode(dataset.id, StandardCharsets.UTF_8.toString());
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
            Table table = catalogAPI.createTable(datasetId, dataset.getPath(), accessControlList, format);
            return table;
        }
    }

    public static Builder builder(CatalogAPI catalogAPI){
        return new Builder(catalogAPI);
    }


}
