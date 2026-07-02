package org.sv.flexobject.dremio.domain.catalog;

import org.sv.flexobject.dremio.DremioClient;
import org.sv.flexobject.dremio.DremioRestApp;
import org.sv.flexobject.dremio.api.CatalogAPI;
import org.sv.flexobject.dremio.domain.Permissions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.*;

public class PriviledgesTest {
    CatalogAPI catalogAPI;

    @BeforeEach
    public void setUp() throws Exception {
        DremioClient enterpriseClient = DremioRestApp.getEnterpriseClient();
        if (enterpriseClient != null) {
            catalogAPI = enterpriseClient.catalog();
        }
    }

    @Test
    public void getPriviledges() throws Exception {
        if (catalogAPI != null) {
            Priviledges priviledges = catalogAPI.getPriviledges();
            System.out.println(priviledges.toJson().toPrettyString());
            EnumSet<Permissions> expectedPermissions = EnumSet.of(
                    Permissions.VIEW_REFLECTION,
                    Permissions.MANAGE_GRANTS,
                    Permissions.ALTER,
                    Permissions.SELECT,
                    Permissions.ALTER_REFLECTION);
            assertEquals(expectedPermissions, priviledges.getPermissions(GrantType.FOLDER));
        }
    }
}