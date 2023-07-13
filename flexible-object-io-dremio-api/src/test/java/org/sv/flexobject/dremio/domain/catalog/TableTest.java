package org.sv.flexobject.dremio.domain.catalog;

import org.sv.flexobject.dremio.DremioRestApp;
import org.sv.flexobject.dremio.api.CatalogAPI;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TableTest {

    CatalogAPI catalogAPI;

    @Before
    public void setUp() throws Exception {
        catalogAPI = DremioRestApp.getClient().catalog();
    }

    @Test
    public void createNYCtaxitripsTable() throws Exception {
        Table table = Table.builder(catalogAPI)
                .startAt("Samples")
                .subFolder("samples.dremio.com")
                .dataset("NYC-taxi-trips")
                .format(null, FormatType.PARQUET, true);
        assertNotNull(table);
        System.out.println(table.toJson().toPrettyString());

        System.out.println("deleting created table " + table.id);
        catalogAPI.delete(table);
    }
}