package org.sv.flexobject.dremio.domain.user;

import org.sv.flexobject.dremio.domain.ApiData;
import org.sv.flexobject.schema.annotations.FieldName;
import org.sv.flexobject.schema.annotations.ValueClass;

import java.util.ArrayList;
import java.util.List;

public class User extends ApiData {
    @FieldName(name = "@type")
    public String type;
    public String id;
    public String name;
    public String firstName;
    public String lastName;
    public String email;

    @ValueClass(valueClass = Role.class)
    public List<Role> roles = new ArrayList<>();

    public String source;
    public Boolean active;
    public String tag;

    public static class Builder {
        User user = new User();

        public Builder firstName(String firstName){
            user.firstName = firstName;
            return this;
        }
        public Builder lastName(String lastName){
            user.lastName = lastName;
            return this;
        }
        public Builder name(String name){
            user.name = name;
            return this;
        }

        public Builder email(String email){
            user.email = email;
            return this;
        }
        public Builder id(String id){
            user.id = id;
            return this;
        }
        public Builder type(String type){
            user.type = type;
            return this;
        }

        public Builder addRole(Role role){
            user.roles.add(role);
            return this;
        }

        public User build(){
            return user;
        }
    }

    public static Builder builder(){
        return new Builder();
    }
}
