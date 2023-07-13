package org.sv.flexobject.dremio.api;


import org.apache.commons.lang3.StringUtils;
import org.sv.flexobject.schema.DataTypes;

public class AuthAPI {
    Session session;

    public static class AuthRequest extends DremioApiRequest {
        String userName;
        String password;

        public AuthRequest(String userName, Object password) {
            this.userName = userName;
            try {
                this.password = DataTypes.stringConverter(password);
            } catch (Exception e) {
                throw new DremioApiException("Unusable password", e);
            }
        }
    }

    public static class AuthResponse extends DremioApiResponse {
        String token;

        public AuthResponse() {
        }

        public String getToken() {
            return token;
        }

        public boolean isSuccess(){
            return isRestOk() && StringUtils.isNotBlank(getToken());
        }
    }

    public AuthAPI(Session session) {
        this.session = session;
    }

    public String authenticate(Object password) {
        AuthRequest request = new AuthRequest(session.getConf().getUsername(), password);
        AuthResponse response = session.submit("apiv2", request, "login", AuthResponse.class);
        if (response.isSuccess())
            return response.getToken();
        throw response.getError();
    }
}
