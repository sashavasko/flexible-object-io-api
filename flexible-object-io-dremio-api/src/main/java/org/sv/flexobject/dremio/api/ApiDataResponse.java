package org.sv.flexobject.dremio.api;

import org.sv.flexobject.dremio.domain.ApiData;
import org.sv.flexobject.schema.annotations.NonStreamableField;
import org.sv.flexobject.util.InstanceFactory;

public class ApiDataResponse extends DremioApiResponse{
    @NonStreamableField
    Class<? extends ApiData> dataClass;
    ApiData data;

    public ApiDataResponse(Class<? extends ApiData> dataClass) {
        this.dataClass = dataClass;
    }

    public Class<? extends ApiData> getDataClass(){
       return dataClass;
    }

    public <T extends ApiData> T getData() {
        if (data == null && fullResponse != null && !fullResponse.isEmpty()){
            try{
                data = InstanceFactory.get(getDataClass());
                data.fromJson(fullResponse);
            } catch (Exception e) {
                throw new DremioApiException("Failed to parse REST response body:", e);
            }
        }

        return (T) dataClass.cast(data);
    }

    @Override
    public boolean isSuccess(){
        return isRestOk() && !getData().isEmpty();
    }
}

