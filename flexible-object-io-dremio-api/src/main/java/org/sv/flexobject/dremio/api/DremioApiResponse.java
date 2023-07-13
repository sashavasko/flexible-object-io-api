package org.sv.flexobject.dremio.api;

import com.bettercloud.vault.rest.RestResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.commons.lang3.StringUtils;
import org.sv.flexobject.StreamableImpl;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.schema.annotations.NonStreamableField;

public class DremioApiResponse  extends StreamableImpl {
    public static final int REST_STATUS_OK = 200;
    public static final int REST_STATUS_OK_NO_CONTENT = 204;
    @NonStreamableField
    int status;
    String errorMessage;
    String moreInfo;

    @NonStreamableField
    JsonNode fullResponse;

    public DremioApiResponse() {
    }

    public DremioApiResponse forRestResponse(RestResponse restResponse){
        status = restResponse.getStatus();
        if (!isNoContent() && restResponse.getBody().length > 0) {
            try {
                if ("application/json".equals(restResponse.getMimeType())) {
                    fullResponse = MapperFactory.getObjectReader().readTree(restResponse.getBody());
                    this.fromJsonBytes(restResponse.getBody());
                } else {
                    errorMessage = new String(restResponse.getBody());
                    fullResponse = JsonNodeFactory.instance.objectNode().put("errorMessage", errorMessage);
                }
            } catch (Exception e) {
                throw new DremioApiException("Failed to parse response body JSON:" + new String(restResponse.getBody()), e);
            }
        }
        return this;
    }

    public boolean isRestOk(){
        return getStatus() == REST_STATUS_OK || getStatus() == REST_STATUS_OK_NO_CONTENT;
    }

    public boolean isNoContent(){
        return getStatus() == REST_STATUS_OK_NO_CONTENT;
    }

    public int getStatus() {
        return status;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getMoreInfo() {
        return moreInfo;
    }

    public DremioApiException getError(){
        StringBuilder sb = new StringBuilder("Rest API returned ");
        if (!isRestOk()) {
            String responseCode;
            switch(getStatus()){
                case 200 : responseCode = "200 OK"; break;
                case 400 : responseCode = "400 Bad Request"; break;
                case 401 : responseCode = "401 Unauthorized"; break;
                case 404 : responseCode = "404 Not Found"; break;
                case 405 : responseCode = "405 Method Not Allowed"; break;
                case 409 : responseCode = "409 Conflict"; break;
                default: responseCode = Integer.toString(getStatus());
            }
            sb.append(responseCode);
            if (StringUtils.isNotBlank(getErrorMessage()))
                sb.append(" with error message:").append(getErrorMessage());
            if (StringUtils.isNotBlank(getMoreInfo()))
                sb.append("(").append(getMoreInfo()).append(")");
        } else {
            sb.append("Ok, but request resulted in error");
            if (StringUtils.isNotBlank(getErrorMessage()))
                sb.append(":").append(getErrorMessage());
            if (StringUtils.isNotBlank(getMoreInfo()))
                sb.append("(").append(getMoreInfo()).append(")");
        }
        return new DremioApiException(sb.toString(), getStatus());
    }

    public boolean isSuccess(){
        return isRestOk();
    }

}
