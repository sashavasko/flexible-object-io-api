package org.sv.flexobject.dremio.api;

import org.sv.flexobject.dremio.DremioClientConf;
import org.sv.flexobject.dremio.domain.user.Role;
import org.sv.flexobject.dremio.domain.user.User;
import org.sv.flexobject.schema.annotations.FieldName;
import org.sv.flexobject.schema.annotations.ValueClass;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class UserAPI {
    Session session;

    public UserAPI() {
    }

    public UserAPI forSession(Session session) {
        this.session = session;
        return this;
    }

    public static class UserListResponse extends DremioApiResponse{
        @ValueClass(valueClass = User.class)
        public List<User> data = new ArrayList<>();
        @ValueClass(valueClass = User.class)
        public List<User> users = new ArrayList<>();

        public List<User> getData() {
            return data.isEmpty() ? users : data;
        }
    }

    public static class CreateUserRequest extends UserRequest{
        @FieldName(name = "@type")
        public String type;
        public String password;

        public CreateUserRequest(User user, String password) {
            super(user);
            this.type = user.type;
            this.password = password;
        }
    }

    public static class UpdateUserRequest extends UserRequest{
        public String tag;

        public UpdateUserRequest(User user) {
            super(user);
            tag = user.tag;
        }
    }
    public static class FirstUserRequest extends DremioApiRequest{
        public String userName;
        public String firstName;
        public String lastName;
        public String email;
        public String password;
        public Timestamp createdAt = new Timestamp(System.currentTimeMillis());

        public FirstUserRequest(User user, String password) {
            setRequestType(RequestTypes.put);
            this.userName = user.name;
            this.firstName = user.firstName;
            this.lastName = user.lastName;
            this.email = user.email;
            this.password = password;
        }
    }

    public static class UserRequest extends DremioApiRequest{
        public String id;
        public String name;
        public String firstName;
        public String lastName;
        public String email;

        @ValueClass(valueClass = Role.class)
        public List<Role> roles = new ArrayList<>();

        public UserRequest(User user) {
            this.id = user.id;
            this.name = user.name;
            this.firstName = user.firstName;
            this.lastName = user.lastName;
            this.email = user.email;
            this.roles = user.roles;
        }
    }
    public static class UserResponse extends ApiDataResponse{
        public User data;

        public UserResponse() {
            super(User.class);
        }
    }

    public List<User> listUsers() throws Exception {
        DremioApiRequest request = new DremioApiRequest().setRequestType(DremioApiRequest.RequestTypes.get);
        UserListResponse response = session.submit("apiv2",request, "user", UserListResponse.class);
        if (response.isSuccess())
            return response.getData();
        else if (response.getStatus() == 404){
            response = session.submit("apiv2",request, "users/all", UserListResponse.class);
            if (response.isSuccess()) {
                return response.getData();
            }
        }
        throw response.getError();
    }

    /**
     To be used for testing.
     First user created is the admin of the Dremio.
     */
    public static User firstUser(DremioClientConf conf, User user, String password){
        Session unauthorizedSession = new Session(conf);
        ApiDataResponse response = unauthorizedSession.submit("apiv2", new FirstUserRequest(user, password), "bootstrap/firstuser", UserResponse.class);
        if (response.isSuccess())
            return response.getData();
        throw response.getError();
    }

    public User createGetOrUpdateUser(String path, DremioApiRequest request, Class<? extends ApiDataResponse> responseClass) {
        ApiDataResponse response = session.submit(request, path, responseClass);
        if (response.isSuccess())
            return response.getData();
        throw response.getError();
    }

    public User create(User user, String password){
        return createGetOrUpdateUser("user", new CreateUserRequest(user, password), UserResponse.class);
    }

    public User get(String id){
        DremioApiRequest request = new DremioApiRequest().setRequestType(DremioApiRequest.RequestTypes.get);
        return createGetOrUpdateUser("user/" + id, request, UserResponse.class);
    }

    public User getByName(String name) {
        String urlEncodedName = name;
        try {
            urlEncodedName = URLEncoder.encode(name, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
        }
        DremioApiRequest request = new DremioApiRequest().setRequestType(DremioApiRequest.RequestTypes.get);
        return createGetOrUpdateUser("user/by-name/" + urlEncodedName, request, UserResponse.class);
    }

    public User update(User user){
        return createGetOrUpdateUser("user", new UpdateUserRequest(user), UserResponse.class);
    }

    public User delete(User user){
        DremioApiRequest request = new DremioApiRequest().setRequestType(DremioApiRequest.RequestTypes.delete);
        String version = null;
        try{
            version = URLEncoder.encode(user.tag, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException ex) {
        }
        try {
            return createGetOrUpdateUser("user/" + user.id + "?version=" + version, request, UserResponse.class);
        }catch (DremioApiException e){
            if (e.getRestApiReturnCode() == 405) {
                String path = String.format("user/%s/?version=%s", user.name, version);
                ApiDataResponse response = session.submit("apiv2", request, path, UserResponse.class);
                if (response.isRestOk())
                    return user;
                throw response.getError();
            } else {
                throw e;
            }
        }
    }

}
