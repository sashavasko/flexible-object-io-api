package org.sv.flexobject.dremio.api;

import org.sv.flexobject.dremio.DremioRestApp;
import org.sv.flexobject.dremio.domain.user.User;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;

public class UserAPITest {
    UserAPI userAPI;

    @Before
    public void setUp() throws Exception {
        userAPI = DremioRestApp.getClient().user();
    }

    @Test
    public void listUsers() throws Exception {
        List<User> users = userAPI.listUsers();
        Optional<User> serviceUser = users.stream().filter(u-> u.name.equals("testUser")).findFirst();
        assertTrue(serviceUser.isPresent());
        // The following only works on Enterprise version of dremio, which is not what is in dockerized version
        // assertEquals("EnterpriseUser", serviceUser.get().type);
    }

    @Test
    public void firstUser() throws Exception {
        User firstUser = User.builder()
                .firstName("foo")
                .lastName("bar")
                .email("foobar@foo.bar")
                .build();
        try {
            UserAPI.firstUser(DremioRestApp.getDremioConf(), firstUser, "bozo1234");
            throw new RuntimeException("Must have thrown an exception!");
        } catch (DremioApiException e){
            assertEquals("Rest API returned 400 Bad Request with error message:First user can only be created when no user is already registered", e.getMessage());
        }
    }

    @Test
    public void createAndDeleteUser() throws InterruptedException {
        User user = User.builder()
                .name("FooBarUser")
                .firstName("foo")
                .lastName("bar")
                .email("foobar@foo.bar")
                .build();
        User createdUser = userAPI.create(user, "BarFoo456$");

        assertEquals("foobar@foo.bar", createdUser.email);
        userAPI.delete(createdUser);
    }

}