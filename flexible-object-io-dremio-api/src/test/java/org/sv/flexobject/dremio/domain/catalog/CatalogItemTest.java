package org.sv.flexobject.dremio.domain.catalog;

import org.junit.Test;

import java.sql.Timestamp;

import static org.junit.Assert.*;

public class CatalogItemTest {

    @Test
    public void fromJsonCreatedAt() throws Exception {
        CatalogItem item = new CatalogItem();
        String json = "{\"createdAt\" : \"2023-04-27T15:33:08.082Z\"}";
        item.fromJsonBytes(json.getBytes());
        assertEquals(Timestamp.valueOf("2023-04-27 10:33:08.082"), item.createdAt);
    }
}