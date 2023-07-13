package org.sv.flexobject.dremio.api;

import org.sv.flexobject.dremio.domain.catalog.Dataset;
import org.sv.flexobject.dremio.domain.catalog.Entity;
import org.sv.flexobject.schema.annotations.NonStreamableField;
import org.sv.flexobject.util.InstanceFactory;

public class EntityResponse extends DremioApiResponse{
    @NonStreamableField
    Class<? extends Entity> entityClass;
    Entity data;

    public EntityResponse() {
    }

    public EntityResponse(Class<? extends Entity> entityClass) {
        this.entityClass = entityClass;
    }

    public Class<? extends Entity> getEntityClass(){
        Entity header = new Entity();
        try {
            if (entityClass == null) {
                header.fromJson(fullResponse);
                entityClass = header.entityType.getEntityClass();
            }
            return entityClass;
        } catch (Exception e) {
            throw new DremioApiException("Failed to parse REST response body:", e);
        }
    }

    public <T extends Entity> T getData() {
        if (data == null
                && fullResponse != null
                && fullResponse.isContainerNode()
                && fullResponse.has("entityType")){
            try {
                data = InstanceFactory.get(getEntityClass());
                data.fromJson(fullResponse);
                if (data instanceof Dataset){
                    entityClass = ((Dataset) data).type.getEntityClass();
                    data = InstanceFactory.get(entityClass);
                    data.fromJson(fullResponse);
                }
            } catch (Exception e) {
                throw new DremioApiException("Failed to parse REST response body:", e);
            }
        }

        return (T) entityClass.cast(data);
    }

    @Override
    public boolean isSuccess(){
        return isRestOk() && !getData().isEmpty();
    }
}

