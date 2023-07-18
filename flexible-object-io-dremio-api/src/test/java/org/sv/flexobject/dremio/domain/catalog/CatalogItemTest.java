package org.sv.flexobject.dremio.domain.catalog;

import org.junit.Ignore;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.*;

public class CatalogItemTest {

    @Test
    public void fromJsonCreatedAt() throws Exception {
        CatalogItem item = new CatalogItem();
        String json = "{\"createdAt\" : \"2023-04-27T15:33:08.082Z\"}";
        item.fromJsonBytes(json.getBytes());

        DateTimeFormatter f = DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.systemDefault());
        LocalDateTime expectedLocalDateTime = LocalDateTime.parse("2023-04-27T15:33:08.082Z", f);


        assertEquals(Timestamp.valueOf(expectedLocalDateTime), item.createdAt);
    }
}