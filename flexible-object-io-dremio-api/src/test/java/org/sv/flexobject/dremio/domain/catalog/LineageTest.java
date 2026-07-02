package org.sv.flexobject.dremio.domain.catalog;

import org.sv.flexobject.dremio.DremioClient;
import org.sv.flexobject.dremio.DremioRestApp;
import org.sv.flexobject.dremio.api.CatalogAPI;
import org.sv.flexobject.dremio.domain.catalog.lineage.Dataset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class LineageTest {
    CatalogAPI catalogAPI;

    @BeforeEach
    public void setUp() throws Exception {
        DremioClient enterpriseClient = DremioRestApp.getEnterpriseClient();
        if (enterpriseClient != null) {
            catalogAPI = enterpriseClient.catalog();
        }
    }

    @Test
    public void getVinsLineage() throws Exception {
        if (catalogAPI != null) {
            View view = View.builder(catalogAPI)
                    .path("TestSpace", "zips")
                    .get();
            Lineage lineage = catalogAPI.lineage(view);
            Dataset parent = lineage.parents.get(0);
            System.out.println(lineage.toJson().toPrettyString());
            assertEquals("zips.json", parent.path.get(parent.path.size() - 1));
        }
    }
}