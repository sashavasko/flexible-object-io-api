package org.sv.flexobject.dremio.domain.catalog;

import org.sv.flexobject.dremio.api.CatalogAPI;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ValueType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class View extends Dataset{
    public String sql; // SQL query used to create the view
    @ValueType(type = DataTypes.string)
    public List<String> sqlContext = new ArrayList<>(); //Context for the SQL query used to create the view.

    public View() {
        super();
        type = TableType.VIRTUAL_DATASET;
    }

    public static class Builder {
        CatalogAPI catalogAPI;
        List<String> path;
        String sql;
        List<String> contextPath;


        protected Builder(CatalogAPI catalogAPI) {
            this.catalogAPI = catalogAPI;
        }

        public Builder path(List<String> path){
            this.path = path;
            return this;
        }

        public Builder path(String ... path){
            this.path = Arrays.asList(path);
            return this;
        }

        public Builder sql(String sql){
            this.sql = sql;
            return this;
        }

        public Builder context(List<String> context){
            this.contextPath = context;
            return this;
        }
        public Builder context(String ... context){
            return context(Arrays.asList(context));
        }
        public Builder context(Dataset context){
            return context(context.getContext());
        }

        public View get() {
            return catalogAPI.catalog().traversePath(catalogAPI, path, View.class);
        }

        public View create(AccessControlList accessControlList, boolean reCreateIfExists){
            View view = get();
            if (view != null && reCreateIfExists) {
                catalogAPI.delete(view);
                view = null;
            }

            if (view == null){
                view = catalogAPI.createView(path,
                        accessControlList,
                        sql,
                        contextPath);
            }
            return view;
        }
    }

    public static Builder builder(CatalogAPI catalogAPI){
        return new Builder(catalogAPI);
    }
}
