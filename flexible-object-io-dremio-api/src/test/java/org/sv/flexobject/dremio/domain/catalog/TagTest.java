package org.sv.flexobject.dremio.domain.catalog;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.sv.flexobject.dremio.DremioRestApp;
import org.sv.flexobject.dremio.api.CatalogAPI;
import org.sv.flexobject.dremio.api.DremioApiException;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class TagTest {
    CatalogAPI catalogAPI;

    @Before
    public void setUp() throws Exception {
        catalogAPI = DremioRestApp.getClient().catalog();
    }

    @Test
    public void createUpdateDeleteVinsTag() throws Exception {
        View view = View.builder(catalogAPI)
                .path("TestSpace", "zips")
                .get();
        Tag tag = catalogAPI.tag(view);
        if (tag == null)
           tag = catalogAPI.createTag(view, Arrays.asList("zips"));
        else
           tag = catalogAPI.updateTag(view, Arrays.asList("zips"), tag.version);

        assertEquals(Arrays.asList("zips"), tag.tags);
        assertTrue(StringUtils.isNotBlank(tag.version));
        tag = catalogAPI.updateTag(view, Arrays.asList("zips","test"), tag.version);
        assertTrue(StringUtils.isNotBlank(tag.version));
        tag = catalogAPI.deleteTag(view, tag.version);
        System.out.println(tag);
    }

}