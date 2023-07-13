package org.sv.flexobject.dremio.api;

import org.sv.flexobject.dremio.domain.ApiData;
import org.sv.flexobject.dremio.domain.catalog.*;
import org.sv.flexobject.dremio.domain.catalog.config.SourceConf;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.sv.flexobject.schema.annotations.ValueClass;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CatalogAPI {
    Session session;

    public CatalogAPI() {
    }

    public CatalogAPI forSession(Session session){
        this.session = session;
        return this;
    }

    public static class CatalogResponse extends DremioApiResponse{
        @ValueClass(valueClass = CatalogItem.class)
        List<CatalogItem> data = new ArrayList<>();

        public List<CatalogItem> getData() {
            return data;
        }

        @Override
        public boolean isSuccess(){
            return isRestOk() && !data.isEmpty();
        }
    }

    public static class LineageResponse extends ApiDataResponse {
        public LineageResponse() { super(Lineage.class); }
    }

    public static class TagResponse extends ApiDataResponse {
        public TagResponse() { super(Tag.class); }
    }

    public static class WikiResponse extends ApiDataResponse {
        public WikiResponse() { super(Wiki.class); }
    }

    public static class PriviledgesResponse extends ApiDataResponse {
        public PriviledgesResponse() { super(Priviledges.class); }
    }

    public static class GrantsResponse extends ApiDataResponse {
        public GrantsResponse() { super(Grants.class); }
    }

    public static class SourceResponse extends EntityResponse{
        public SourceResponse() {
            super(Source.class);
        }
    }

    public static class SpaceResponse extends EntityResponse{
        public SpaceResponse() {
            super(Space.class);
        }
    }

    public static class CreateDatasetRequest extends DremioApiRequest {
        public EntityType entityType = EntityType.dataset;
        public List<String> path;
        public TableType type;
        public AccessControlList accessControlList;

        public CreateDatasetRequest(TableType type, List<String> path, AccessControlList accessControlList) {
            this.type = type;
            this.path = path;
            this.accessControlList = accessControlList;
        }
    }

    public static class CreateOrUpdateTagRequest extends DremioApiRequest {
        public List<String> tags;
        public String version;

        public CreateOrUpdateTagRequest(List<String> tags, String version) {
            this.tags = tags;
            this.version = version;
        }
    }
    public static class CreateOrUpdateWikiRequest extends DremioApiRequest {
        public String text;
        public Integer version;

        public CreateOrUpdateWikiRequest(String text, Integer version) {
            this.text = text;
            this.version = version;
        }
    }

    public static class CreateTableRequest extends CreateDatasetRequest {
        public Format format;
        public CreateTableRequest(List<String> path, AccessControlList accessControlList, Format format) {
            super(TableType.PHYSICAL_DATASET, path, accessControlList);
            this.format = format;
        }
    }

    public static class CreateViewRequest extends CreateDatasetRequest {
        public String sql;
        public List<String> sqlContext;
        public CreateViewRequest(List<String> path, AccessControlList accessControlList, String sql, String ... sqlContext) {
            this(path, accessControlList, sql, Arrays.asList(sqlContext));
        }
        public CreateViewRequest(List<String> path, AccessControlList accessControlList, String sql, List<String> sqlContext) {
            super(TableType.VIRTUAL_DATASET, path, accessControlList);
            this.sql = sql;
            this.sqlContext = sqlContext;
        }
    }

    public static class CreateFolderRequest extends DremioApiRequest {
        public EntityType entityType = EntityType.folder;
        public List<String> path;
        public AccessControlList accessControlList;

        public CreateFolderRequest(List<String> path, AccessControlList accessControlList) {
            this.path = path;
            this.accessControlList = accessControlList;
        }
    }

    public static class CreateSpaceRequest extends DremioApiRequest {
        public EntityType entityType = EntityType.space;
        public String name;
        public AccessControlList accessControlList;

        public CreateSpaceRequest(String name, AccessControlList accessControlList) {
            this.name = name;
            this.accessControlList = accessControlList;
        }
    }
    public static class CreateSourceRequest extends DremioApiRequest {
        public EntityType entityType = EntityType.source;
        public JsonNode config;
        public String type;
        public String name;
        public MetadataPolicy metadataPolicy;
        public AccessControlList accessControlList;
        Long accelerationGracePeriodMs;
        Long accelerationRefreshPeriodMs;
        Boolean accelerationNeverExpire;
        Boolean accelerationNeverRefresh;
        Boolean allowCrossSourceSelection;
        Boolean disableMetadataValidityCheck;

        public CreateSourceRequest(String name, SourceConf config) {
            setRequestType(DremioApiRequest.RequestTypes.post);
            try {
                this.config = config.toJson();
            } catch (Exception e) {
                throw new DremioApiException("Failed to convert Source config to Json", e);
            }
            this.type = config.getType();
            this.name = name;
        }
    }

    public static class UpdateSourceRequest extends CreateSourceRequest {
        String id;
        String tag;

        public UpdateSourceRequest(Source source) {
            super(source.name, source.getConfig());
            setRequestType(DremioApiRequest.RequestTypes.put);
            this.config = source.config;
            this.type = source.type.name();
            this.name = source.name;
            this.tag = source.tag;
            this.id = source.id;
        }
    }

    public static class UpdateSpaceRequest extends CreateSpaceRequest {
        String id;
        String tag;
        public UpdateSpaceRequest(Space space) {
            super(space.name, space.accessControlList);
            this.tag = space.tag;
            this.id = space.id;
            setRequestType(DremioApiRequest.RequestTypes.put);
        }
    }

    public static class UpdateFolderRequest extends CreateFolderRequest {
        String id;
        String tag;
        public UpdateFolderRequest(Folder folder) {
            super(folder.path, folder.accessControlList);
            this.tag = folder.tag;
            this.id = folder.id;
            setRequestType(DremioApiRequest.RequestTypes.put);
        }
    }

    public static class UpdateGrantsRequest extends DremioApiRequest {
        @ValueClass(valueClass = Grant.class)
        public List<Grant> grants;

        public UpdateGrantsRequest(List<Grant> grants) {
            setRequestType(DremioApiRequest.RequestTypes.put);
            this.grants = grants;
        }
    }

    public Catalog catalog(){
        DremioApiRequest request = new DremioApiRequest().setRequestType(DremioApiRequest.RequestTypes.get);
        CatalogResponse response = session.submit(request, "catalog", CatalogResponse.class);
        if (response.isSuccess())
            return new Catalog(response.getData());
        throw response.getError();
    }

    public <T extends Entity> T getById(String uuid){
        return getById(uuid, true, false);
    }

    public <T extends Entity> T getById(String uuid, boolean includePermissions, boolean excludeChildren){
        if (uuid.contains(":") || uuid.contains("/") || uuid.contains(" ") ) {
            try {
                uuid = URLEncoder.encode(uuid, StandardCharsets.UTF_8.toString());
            } catch (UnsupportedEncodingException e) {
                throw new DremioApiException("Failed to urlencode id" + uuid, e);
            }
        }
        return getEntity("catalog/" + uuid, includePermissions, excludeChildren);
    }

    /**
     *
     * @param urlEncodedPath Path of the Entity that you want to retrieve,
     *                       with a forward slash to separate each level of nesting.
     *                       URI-encode the path to replace special characters in
     *                       folder names with their UTF-8 equivalents,
     *                       such as %3A for a colon and %20 for a space
     *                       (for example, Samples/samples.dremio.com/Dremio%20University).
     * @return Entity retrieved
     */
    public <T extends Entity> T getByPath(String urlEncodedPath){
        return getByPath(urlEncodedPath, true, false);
    }

    public <T extends Entity> T getByPath(String urlEncodedPath, boolean includePermissions, boolean excludeChildren){
        return getEntity("catalog/by-path/" + urlEncodedPath, includePermissions, excludeChildren);
    }

    public <T extends Entity> T getEntity(String path, boolean includePermissions, boolean excludeChildren){
        DremioApiRequest request = new DremioApiRequest().setRequestType(DremioApiRequest.RequestTypes.get);
        StringBuilder sb = new StringBuilder(path);
        if (includePermissions)
            sb.append("?include=permissions");
        if (excludeChildren)
            sb.append(includePermissions? ',' : '?').append("exclude=children");

        EntityResponse response = session.submit(request, sb.toString(), EntityResponse.class);
        if (response.isSuccess())
            return response.getData();
        throw response.getError();
    }

    public Source createSource(String name, SourceConf conf) {
        return createEntity(new CreateSourceRequest(name, conf));
    }

    public Space createSpace(String name) {
        return createSpace(name, null);
    }
    public Space createSpace(String name, AccessControlList accessControlList) {
        return createEntity(new CreateSpaceRequest(name, accessControlList));
    }

    public Folder createFolder(String ... path) {
        return createFolder(Arrays.asList(path), null);
    }

    public Folder createFolder(List<String> path, AccessControlList accessControlList) {
        return createEntity(new CreateFolderRequest(path, accessControlList));
    }
    public Table createTable(String sourceId, List<String> path, AccessControlList accessControlList, Format format) {
        return createEntity(new CreateTableRequest(path, accessControlList, format), sourceId);
    }

    public View createView(List<String> path, AccessControlList accessControlList, String sql, String ... sqlContext) {
        return createEntity(new CreateViewRequest(path, accessControlList, sql, sqlContext));
    }
    public View createView(List<String> path, AccessControlList accessControlList, String sql, List<String> sqlContext) {
        return createEntity(new CreateViewRequest(path, accessControlList, sql, sqlContext));
    }

    public <T extends Entity> T createEntity(DremioApiRequest request) {
        return createEntity(request, null);
    }
    public <T extends Entity> T createEntity(DremioApiRequest request, String urlEncodedPath) {
        String path = StringUtils.isNotBlank(urlEncodedPath) ? "catalog/" + urlEncodedPath : "catalog";
        EntityResponse response = session.submit(request, path, EntityResponse.class);
        if (response.isSuccess())
            return response.getData();
        throw response.getError();
    }

    public <T extends Entity> T update(Entity entity) {
        DremioApiRequest request;
        switch (entity.entityType) {
            case source : request = new UpdateSourceRequest((Source) entity); break;
            case space : request = new UpdateSpaceRequest((Space) entity); break;
            case folder : request = new UpdateFolderRequest((Folder) entity); break;
            default: throw new UnsupportedOperationException("unsupported entity type " + entity.entityType);
        }
        String id = entity.id;
        EntityResponse response = session.submit(request, "catalog/" + id, EntityResponse.class);
        if (response.isSuccess())
            return response.getData();
        throw response.getError();
    }

    public boolean delete(Entity entity) {
        return delete(entity.id);
    }

    public boolean delete(String id) {
        DremioApiRequest request = new DremioApiRequest().setRequestType(DremioApiRequest.RequestTypes.delete);
        DremioApiResponse response = session.submit(request, "catalog/" + id, DremioApiResponse.class);
        if (response.getStatus() == 204)
            return true;
        else
            throw response.getError();
    }

    protected <T extends ApiData> T getCatalogData(String id, String path, Class<? extends ApiDataResponse> responseClass) {
        DremioApiRequest request = new DremioApiRequest().setRequestType(DremioApiRequest.RequestTypes.get);
        ApiDataResponse response = session.submit(request, "catalog/" + id + path, responseClass);
        if (response.isSuccess())
            return response.getData();
        else
            throw response.getError();
    }

    public Lineage lineage(Entity entity) {
        return lineage(entity.id);
    }

    public Lineage lineage(String id) {
        return getCatalogData(id, "/graph", LineageResponse.class);
    }

    public Tag tag(Entity entity) {
        return tag(entity.id);
    }
    public Tag tag(String id) {
        return getCatalogData(id, "/collaboration/tag", TagResponse.class);
    }

    public Tag createTag(Entity entity, List<String> tags) {
        return createTag(entity.id, tags);
    }
    public Tag createTag(String id, List<String> tags) {
        return createOrUpdateTag(id, tags, null);
    }

    public Tag updateTag(Entity entity, List<String> tags, String version) {
        return updateTag(entity.id, tags, version);
    }

    public Tag updateTag(String id, List<String> tags, String version) {
        return createOrUpdateTag(id, tags, version);
    }

    public Tag deleteTag(Entity entity, String version) {
        return deleteTag(entity.id, version);
    }
    public Tag deleteTag(String id, String version) {
        return createOrUpdateTag(id, Collections.emptyList(), version);
    }
    protected Tag createOrUpdateTag(String id, List<String> tags, String version) {
        return createOrUpdateCatalogData("catalog/" + id + "/collaboration/tag",
                new CreateOrUpdateTagRequest(tags, version),
                TagResponse.class);
    }

    public Wiki wiki(Entity entity) {
        return wiki(entity.id);
    }
    public Wiki wiki(String id) {
        return getCatalogData(id, "/collaboration/wiki", WikiResponse.class);
    }

    public Wiki createWiki(Entity entity, String text) {
        return createWiki(entity.id, text);
    }
    public Wiki createWiki(String id, String text) {
        return createOrUpdateWiki(id, text, null);
    }

    public Wiki updateWiki(Entity entity, String text, int version) {
        return updateWiki(entity.id, text, version);
    }

    public Wiki updateWiki(String id, String text, int version) {
        return createOrUpdateWiki(id, text, version);
    }

    public Wiki deleteWiki(Entity entity, int version) {
        return deleteWiki(entity.id, version);
    }
    public Wiki deleteWiki(String id, int version) {
        return createOrUpdateWiki(id, "", version);
    }
    protected Wiki createOrUpdateWiki(String id, String text, Integer version) {
        return createOrUpdateCatalogData("catalog/" + id + "/collaboration/wiki",
                new CreateOrUpdateWikiRequest(text, version),
                WikiResponse.class);
    }

    protected <T extends ApiData> T createOrUpdateCatalogData(String path, DremioApiRequest request, Class<? extends ApiDataResponse> responseClass) {
        ApiDataResponse response = session.submit(request, path, responseClass);
        if (response.isSuccess())
            return response.getData();
        throw response.getError();
    }

    public Priviledges getPriviledges(){
        DremioApiRequest request = new DremioApiRequest().setRequestType(DremioApiRequest.RequestTypes.get);
        PriviledgesResponse response = session.submit(request, "catalog/privileges", PriviledgesResponse.class);
        if (response.isSuccess())
            return response.getData();
        throw response.getError();
    }

    public Grants grants(Entity entity) {
        return grants(entity.id);
    }
    public Grants grants(String id) {
        return getCatalogData(id, "/grants", GrantsResponse.class);
    }

    public boolean updateGrants(Entity entity, List<Grant> grants) {
        return updateGrants(entity.id, grants);
    }
    public boolean updateGrants(String id, List<Grant> grants) {
        UpdateGrantsRequest request = new UpdateGrantsRequest(grants);
        DremioApiResponse response = session.submit(request, "catalog/" + id + "/grants", DremioApiResponse.class);
        return response.isSuccess();
    }

}
