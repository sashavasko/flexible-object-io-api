package org.sv.flexobject.dremio.api;

import org.sv.flexobject.dremio.DremioRestApp;
import org.sv.flexobject.dremio.domain.catalog.config.HdfsConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sv.flexobject.dremio.domain.catalog.*;
import org.sv.flexobject.json.MapperFactory;

import java.util.Optional;

import static org.junit.Assert.*;

public class CatalogAPITest {
    CatalogAPI catalogAPI;
    String username;
    String entityToDelete = null;

    @Before
    public void setUp() {
        catalogAPI = DremioRestApp.getClient().catalog();
        username = DremioRestApp.getClient().getConf().getUsername();
    }

    @After
    public void tearDown() throws Exception {
        if (entityToDelete != null){
            catalogAPI.delete(entityToDelete);
            entityToDelete = null;
        }
    }

    @Test
    public void catalog() throws Exception {
        Catalog catalog = catalogAPI.catalog();
        System.out.println(MapperFactory.pretty(catalog.toJson()));

        assertFalse(catalog.isEmpty());

        Optional<CatalogItem> home = catalog.findByPath("@" + username);
        assertTrue(home.isPresent());

        assertEquals(ContainerType.HOME, home.get().containerType);
    }

    @Test
    public void createUpdateAndDeleteSpace() throws Exception {
        String spaceName = "foobarTestSpace";
        Space space = catalogAPI.createSpace(spaceName);
        entityToDelete = space.id;

        assertEquals("foobarTestSpace", space.name);
        System.out.println(MapperFactory.pretty(space.toJson()));

//        space.name = "foobarTestSpaceRenamed";
        space = catalogAPI.update(space);
//        assertEquals("foobarTestSpaceRenamed", space.name);
    }

    @Test
    public void getHome() throws Exception {
        Source source = catalogAPI.getByPath("@" + username);

        assertEquals(EntityType.home, source.entityType);

        System.out.println(MapperFactory.pretty(source.toJson()));
    }
    @Test
    public void createUpdateAndDeleteFolder() throws Exception {
        String spaceName = "unitTestSpace";
        Space space = catalogAPI.createSpace(spaceName);
        entityToDelete = space.id;

        Folder folder = catalogAPI.createFolder(spaceName, "foobarFolder");

        assertEquals("foobarFolder", folder.path.get(1));
        System.out.println(MapperFactory.pretty(folder.toJson()));
    }

    @Test
    public void getTestFile() throws Exception {
    }

}