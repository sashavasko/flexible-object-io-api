package org.sv.flexobject.dremio.domain.catalog;

import org.junit.jupiter.api.AfterEach;
import org.sv.flexobject.dremio.DremioRestApp;
import org.sv.flexobject.dremio.api.CatalogAPI;
import org.sv.flexobject.dremio.api.DremioApiException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class WikiTest {
    CatalogAPI catalogAPI;
    Table table;
    View view;

    @BeforeEach
    public void setUp() throws Exception {
        catalogAPI = DremioRestApp.getClient().catalog();
        table = Table.builder(catalogAPI)
                .startAt("Samples")
                .subFolder("samples.dremio.com")
                .dataset("NYC-taxi-trips")
                .format(null, FormatType.PARQUET, false);

        view = View.builder(catalogAPI)
                .path("TestSpace", "NYCTaxiTrips")
                .sql("select * from \"NYC-taxi-trips\"")
                .context(table)
                .create(null, false);
    }

    @AfterEach
    public void tearDown() throws Exception {
        catalogAPI.delete(view);
        catalogAPI.delete(table);
    }

    @Test
    public void createUpdateDeleteVinsWiki() throws Exception {
        assertNotNull(view);
        Wiki wiki;
        wiki = catalogAPI.createWiki(view, "This is a list of zip codes");
        assertEquals("This is a list of zip codes", wiki.text);
        assertTrue(wiki.version >= 0);
        int oldVersion = wiki.version;
        wiki = catalogAPI.updateWiki(view, "foo bar", wiki.version);
        assertTrue(oldVersion < wiki.version);
        wiki = catalogAPI.deleteWiki(view, wiki.version);
        System.out.println(wiki);
    }

}