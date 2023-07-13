package org.sv.flexobject.dremio.api;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sv.flexobject.StreamableImpl;
import org.sv.flexobject.schema.annotations.NonStreamableField;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DremioApiRequest extends StreamableImpl {

    public static final Logger logger = LogManager.getLogger(DremioApiRequest.class);
    public enum RequestTypes {
        post,
        get,
        put,
        delete
    }

    public DremioApiRequest setRequestType(RequestTypes requestType) {
        this.requestType = requestType;
        return this;
    }

    @NonStreamableField
    Session session;
    @NonStreamableField
    Map<String, String> headers = new HashMap<>();

    @NonStreamableField
    RequestTypes requestType = RequestTypes.post;

    public RequestTypes getRequestType() {
        return requestType;
    }

    public DremioApiRequest() {
    }

    protected byte[] getBodyBytes(){
        try {
            return toJsonBytes();
        } catch (Exception e) {
            throw new DremioApiException("Failed to convert request body to JSON", e);
        }
    }

    public void header(String key, String value){
        headers.put(key, value);
    }

    public void clearHeaders(){
        headers.clear();
    }

    public Set<Map.Entry<String,String>> getHeaders(){
        return headers.entrySet();
    }
}
