package org.sv.flexobject.dremio.api;

import com.bettercloud.vault.rest.Rest;
import com.bettercloud.vault.rest.RestException;
import com.bettercloud.vault.rest.RestResponse;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sv.flexobject.dremio.DremioClientConf;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.util.InstanceFactory;

import java.util.Map;

public class Session {
    public static final Logger logger = LogManager.getLogger(DremioApiRequest.class);

    DremioClientConf conf;
    String token;

    public Session(DremioClientConf conf) {
        this.conf = conf;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public DremioClientConf getConf() {
        return conf;
    }

    protected void addAuthenticationHeaders(Rest rest){
        if (StringUtils.isNotBlank(token))
            header(rest, "Authorization", "_dremio" + token);
        else
            header(rest, "Authorization", "_dremionull");
    }

    public <T extends DremioApiResponse> T submit(DremioApiRequest request, String path, Class<? extends DremioApiResponse> responseClass){
        return submit(null, request, path, responseClass);
    }

    private static Rest header(Rest rest, String key, String value){
        rest.header(key, value);
        logger.debug("HEADER " + key + ": " + value);
        return rest;
    }

    public <T extends DremioApiResponse> T submit(String apiPath, DremioApiRequest request, String path, Class<? extends DremioApiResponse> responseClass){
        DremioApiResponse response = InstanceFactory.get(responseClass);
        byte[] body = request.getBodyBytes();
        RestResponse restResponse = null;
        try {
            String url = conf.getUrl(apiPath) + "/" + path;
            logger.debug(url);
            Rest rest = new Rest()
                    .url(url);
            header(rest, "Content-Type", "application/json");

            rest.sslVerification(false);

            addAuthenticationHeaders(rest);
            for (Map.Entry<String,String> header : request.getHeaders()) {
                header(rest, header.getKey(), header.getValue());
            }
            try {
                ObjectNode json = request.toJson();
                if (json.has("password")){
                    json.put("password", "*********");
                }
                logger.debug("REQUEST (" + request.getRequestType() + "): " + MapperFactory.pretty(json));
            } catch (Exception e) {
            }
            rest.body(body);
            switch(request.getRequestType()) {
                case post: restResponse = rest.post(); break;
                case get:  restResponse = rest.get(); break;
                case put:  restResponse = rest.put(); break;
                case delete:  restResponse = rest.delete(); break;
            }
            logger.debug(String.format("REST status: %d, mime type: %s, body: %s", restResponse.getStatus(), restResponse.getMimeType(), new String(restResponse.getBody())));
            response.forRestResponse(restResponse);
            try {
                logger.debug("RESPONSE: " + MapperFactory.pretty(response.fullResponse));
            } catch (JsonProcessingException e) {
            }
        } catch (RestException e) {
            throw new DremioApiException("Rest request failed with Exception", e);
        }
        return (T) responseClass.cast(response);
    }


    public void authenticate(Object password) {
        token = new AuthAPI(this).authenticate(password);
    }
}
