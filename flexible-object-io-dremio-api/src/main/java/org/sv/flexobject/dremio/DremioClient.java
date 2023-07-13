package org.sv.flexobject.dremio;

import org.sv.flexobject.dremio.api.CatalogAPI;
import org.sv.flexobject.dremio.api.Session;
import org.sv.flexobject.dremio.api.UserAPI;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sv.flexobject.util.InstanceFactory;

public class DremioClient implements AutoCloseable {

    public static final Logger logger = LogManager.getLogger(DremioClient.class);
    DremioClientConf conf = new DremioClientConf();
    Session session;

    public static class Builder {
        DremioClientConf conf;
        Object password;
        String token;

        public Builder forConf(DremioClientConf conf){
            this.conf = conf;
            return this;
        }

        public Builder withPassword(Object password){
            this.password = password;
            return this;
        }

        public Builder forToken(String token){
            this.token = token;
            return this;
        }

        public DremioClient build(){
            DremioClient dremio = new DremioClient();
            dremio.conf = conf == null ? InstanceFactory.get(DremioClientConf.class) : conf;
            dremio.session = new Session(conf);
            if (StringUtils.isNotBlank(token))
                dremio.session.setToken(token);
            else
                dremio.session.authenticate(password);

            return dremio;
        }
    }

    public static Builder builder() {
        return InstanceFactory.get(Builder.class);
    }

    public CatalogAPI catalog(){
        return new CatalogAPI().forSession(session);
    }
    public UserAPI user(){
        return new UserAPI().forSession(session);
    }

    public Session getSession() {
        return session;
    }

    public DremioClientConf getConf() {
        return conf;
    }

    @Override
    public void close() throws Exception {

    }
}
