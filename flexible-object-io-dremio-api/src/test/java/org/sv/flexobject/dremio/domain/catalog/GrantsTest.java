package org.sv.flexobject.dremio.domain.catalog;

import org.sv.flexobject.dremio.DremioClient;
import org.sv.flexobject.dremio.DremioRestApp;
import org.sv.flexobject.dremio.api.CatalogAPI;
import org.sv.flexobject.dremio.api.UserAPI;
import org.sv.flexobject.dremio.domain.Permissions;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static org.junit.Assert.*;

public class GrantsTest {
    CatalogAPI catalogAPI;
    UserAPI userAPI;

    @Before
    public void setUp() throws Exception {
        DremioClient enterpriseClient = DremioRestApp.getEnterpriseClient();
        if (enterpriseClient != null) {
            catalogAPI = enterpriseClient.catalog();
            userAPI = enterpriseClient.user();
        }
    }

    @Test
    public void getGrants() throws Exception {
        if (catalogAPI == null)
            return;
        View view = View.builder(catalogAPI)
                .path("TestSpace", "zips")
                .get();
        Grants grants = catalogAPI.grants(view);
        System.out.println(grants.toJson().toPrettyString());

        EnumSet<Permissions> expectedPermissions = EnumSet.of(
                Permissions.MANAGE_GRANTS,
                Permissions.ALTER,
                Permissions.SELECT);
        assertEquals(expectedPermissions, grants.availablePrivileges);
        Grant grant = new Grant();
        grant.name = "foo";
        grant.setPrivileges(EnumSet.of(Permissions.ALTER));
        grant.granteeType = GranteeType.USER;

        List<Grant> newGrants = Arrays.asList(grant);
        assertTrue(catalogAPI.updateGrants(view, newGrants));

    }
}