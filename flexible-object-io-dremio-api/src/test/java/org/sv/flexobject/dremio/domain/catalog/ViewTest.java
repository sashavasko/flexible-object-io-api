package org.sv.flexobject.dremio.domain.catalog;

import org.sv.flexobject.dremio.DremioRestApp;
import org.sv.flexobject.dremio.api.CatalogAPI;
import org.junit.Before;
import org.junit.Test;

public class ViewTest {
    CatalogAPI catalogAPI;

    @Before
    public void setUp() throws Exception {
        catalogAPI = DremioRestApp.getClient().catalog();
    }

    @Test
    public void createVinsView() throws Exception {
        Table table = Table.builder(catalogAPI)
                .startAt("Samples")
                .subFolder("samples.dremio.com")
                .dataset("NYC-taxi-trips")
                .format(null, FormatType.PARQUET, false);

        View view = View.builder(catalogAPI)
                .path("TestSpace", "NYCTaxiTrips")
                .sql("select * from \"NYC-taxi-trips\"")
                .context(table)
                .create(null, false);
        System.out.println(view.toJson().toPrettyString());
        catalogAPI.delete(view);
        catalogAPI.delete(table);
    }
}