package org.sv.flexobject.dremio.domain.catalog.config;


import org.sv.flexobject.StreamableImpl;
import org.sv.flexobject.schema.annotations.ValueClass;

import java.util.ArrayList;
import java.util.List;

public class SourceConf<SELF extends SourceConf>  extends StreamableImpl {
    public String hostname;
    public Integer port;
    public String accessKey;
    public String accessSecret;
    public String awsProfile;
    public String username;
    public String password;
    public AuthenticationType authenticationType;// = AuthenticationType.MASTER;
    public Integer fetchSize;// = 200;
    public Boolean useSsl;
    public CredentialType credentialType;

    @ValueClass(valueClass = PropertyPair.class)
    public List<PropertyPair> propertyList = new ArrayList<>();

    public SELF addProperty(String name, String value){
        propertyList.add(new PropertyPair(name, value));
        return (SELF) this;
    }

    public SELF setAccessKey(String accessKey, String accessSecret){
        this.accessKey = accessKey;
        this.accessSecret = accessSecret;
        this.credentialType = CredentialType.ACCESS_KEY;
        return (SELF) this;
    }

    public SELF setAwsProfile(String awsProfile){
        this.awsProfile = awsProfile;
        this.credentialType = CredentialType.AWS_PROFILE;
        return (SELF) this;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setAuthenticationType(AuthenticationType authenticationType) {
        this.authenticationType = authenticationType;
    }

    public String getType(){
        return null;
    }
}
