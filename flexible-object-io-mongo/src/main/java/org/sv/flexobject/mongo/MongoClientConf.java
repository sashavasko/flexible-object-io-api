package org.sv.flexobject.mongo;

import com.mongodb.*;
import com.mongodb.connection.*;
import com.mongodb.selector.ServerSelector;
import org.apache.commons.lang3.StringUtils;
import org.sv.flexobject.properties.PropertiesWrapper;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.NonStreamableField;
import org.sv.flexobject.schema.annotations.ValueType;
import org.sv.flexobject.translate.Translator;
import org.sv.flexobject.util.InstanceFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MongoClientConf extends PropertiesWrapper<MongoClientConf> {

    public static final String MONGODB_PREFIX = "mongodb://";
    public static final String MONGODB_SRV_PREFIX = "mongodb+srv://";

    String url;
    @ValueType(type = DataTypes.string)
    List<String> tags;
    @ValueType(type = DataTypes.string)
    List<String> compressorList;
    Long timeout; // used as a default for all other timeouts
    String username;
    String database;
    String readPreference;

    // Cluster settings
    @ValueType(type = DataTypes.string)
    List<String> hosts; // Sets all the specified locations of a Mongo server.
    Long localThresholdMillis;  // Sets the amount of time that a server’s round trip can take and still be eligible for server selection.
    String mode;                // Sets how to connect to a MongoDB server.
    String requiredClusterType; // Sets the type of cluster required for the cluster.
    String requiredReplicaSetName;      // Sets the replica set name required for the cluster.
    Long serverSelectionTimeoutMillis;  // Sets the maximum time to select a primary node before throwing a timeout exception.
    Class<ServerSelector> serverSelector;   // Adds a server selector to apply before server selection.
    String srvHost;         // Sets the host name to use to look up an SRV DNS record to find the MongoDB hosts.
    Integer srvMaxHosts;    // Sets the maximum number of hosts the driver can connect to when using the
                            // DNS seedlist (SRV) connection protocol,
                            // identified by the mongodb+srv connection string prefix.

    // Socket settings:
    Integer connectTimeoutMillis;  // Sets the maximum time to connect to an available socket before throwing a timeout exception.
    Integer readTimeoutMillis;     // Sets the maximum time to read to an available socket before throwing a timeout exception.
    Integer receiveBufferSize;  // Sets the socket's buffer size when receiving.
    Integer sendBufferSize;     // Sets the socket's buffer size when sending.

    // Connection Pool settings :
    Long maintenanceFrequencyMillis;    // Sets the frequency for running a maintenance job.
    Long maintenanceInitialDelayMillis; // Sets the time to wait before running the first maintenance job.
    Long maxConnectionIdleTimeMillis;   // Sets the maximum time a connection can be idle before it's closed.
    Long maxConnectionLifeTimeMillis;   // Sets the maximum time a pooled connection can be alive before it's closed.
    Long maxWaitTimeMillis;             // Sets the maximum time to wait for an available connection.
    Integer maxSize;    // Sets the maximum amount of connections associated with a connection pool.
    Integer minSize;    // Sets the minimum amount of connections associated with a connection pool.

    // Server settings :
    Long heartbeatFrequencyMillis;      // Sets the interval for a cluster monitor to attempt reaching a server.
    Long minHeartbeatFrequencyMillis;   // Sets the minimum interval for server monitoring checks.

    @NonStreamableField
    ConnectionString connectionString;

    @Override
    public MongoClientConf setDefaults() {
        this.timeout = 120000l;
        return this;
    }

    @Override
    public Translator getTranslator() {
        return Translator.identity();
    }


    public List<TagSet> compileTags(){
        List<TagSet> tagsList = new ArrayList<>();
        if (tags != null && !tags.isEmpty()) {
            for (String tagPair : tags) {
                String[] tagValues = tagPair.split(":");
                if (tagValues.length == 2){
                    Tag tag = new Tag(tagValues[0], tagValues[1]);
                    MongoClientProvider.logger.info("Tag : " + tagValues[0] + ":" + tagValues[1]);
                    tagsList.add(new TagSet(tag));
                }
            }
        }
        return tagsList;
    }

    public ReadPreference compileReadPreference(){
        return compileReadPreference(compileTags());
    }
    public ReadPreference compileReadPreference(List<TagSet> tagsList){
        ReadPreference readPreferenceFinal = ReadPreference.secondaryPreferred(tagsList);

        ReadPreference readPreferenceFromUrl = connectionString == null ?
                null :
                connectionString.getReadPreference();

        if (StringUtils.isNotBlank(readPreference)){
            readPreferenceFinal = ReadPreference.valueOf(readPreference, tagsList);
        } else if (readPreferenceFromUrl != null)
            readPreferenceFinal = readPreferenceFromUrl;
        return readPreferenceFinal;
    }

    public MongoClientSettings.Builder makeClientSettingsBuilder(){

        MongoClientSettings.Builder builder = MongoClientSettings.builder();
        if (url != null) {
            MongoClientProvider.logger.info("Connecting to mongo using URL:" + url);
            connectionString = new ConnectionString(url);
            builder.applyConnectionString(connectionString);
        }
        builder.readPreference(compileReadPreference());

        return builder;
    }

    public MongoClientConf applyCompressorList(MongoClientSettings.Builder builder){
        if (compressorList != null && !compressorList.isEmpty()) {
            List<MongoCompressor> compressorListFinal = new ArrayList<>();
            compressorList.forEach(c -> {
                switch (c.toLowerCase()) {
                    case "snappy":
                        compressorListFinal.add(MongoCompressor.createSnappyCompressor());
                        break;
                    case "zlib":
                        compressorListFinal.add(MongoCompressor.createZlibCompressor());
                        break;
                    case "zstd":
                        compressorListFinal.add(MongoCompressor.createZstdCompressor());
                        break;
                }
            });
            if (!compressorListFinal.isEmpty())
                builder.compressorList(compressorListFinal);
        }
        return this;
    }

    private long makeTimeout(Long setting){
        return setting == null ? timeout: setting;
    }

    private int makeTimeout(Integer setting){
        return setting == null ? timeout.intValue(): setting;
    }

    public MongoClientConf applyClusterSettings(ClusterSettings.Builder builder){

        if (hosts != null && !hosts.isEmpty()){
            List<ServerAddress> servers = new ArrayList<>();
            for (String host : hosts){
                if (host.contains(":")){
                    String[] parts = host.split(":");
                    servers.add(new ServerAddress(parts[0], Integer.valueOf(parts[1])));
                } else {
                    servers.add(new ServerAddress(host));
                }
            }
            MongoClientProvider.logger.info("Cluster hosts:" + StringUtils.join(hosts, ' '));
            builder.hosts(servers);
        }

        MongoClientProvider.logger.info("Local Threshold Millis:" + makeTimeout(localThresholdMillis));
        builder.localThreshold(makeTimeout(localThresholdMillis), TimeUnit.MILLISECONDS);

        if (mode != null) {
            MongoClientProvider.logger.info("Cluster connection mode:" + mode);
            builder.mode(ClusterConnectionMode.valueOf(mode));
        }

        if (requiredClusterType != null) {
            MongoClientProvider.logger.info("Cluster type:" + requiredClusterType);
            builder.requiredClusterType(ClusterType.valueOf(requiredClusterType));
        }

        if (requiredReplicaSetName != null) {
            MongoClientProvider.logger.info("Required replica set name:" + requiredReplicaSetName);
            builder.requiredReplicaSetName(requiredReplicaSetName);
        }

        MongoClientProvider.logger.info("Server Selection timeout:" + makeTimeout(serverSelectionTimeoutMillis));
        builder.serverSelectionTimeout(makeTimeout(serverSelectionTimeoutMillis), TimeUnit.MILLISECONDS);

        if (serverSelector != null) {
            MongoClientProvider.logger.info("Server selector:" + serverSelector);
            builder.serverSelector(InstanceFactory.get(serverSelector));
        }

        if (srvHost != null) {
            MongoClientProvider.logger.info("SRV host:" + srvHost);
            builder.srvHost(srvHost);
        }

        if (srvMaxHosts != null) {
            MongoClientProvider.logger.info("SRV max hosts:" + srvMaxHosts);
            builder.srvMaxHosts(srvMaxHosts);
        }

        return this;
    }

    MongoClientConf applySocketSettings(SocketSettings.Builder builder){
        MongoClientProvider.logger.info("Socket Connect timeout Millis:" + makeTimeout(connectTimeoutMillis));
        builder.connectTimeout(makeTimeout(connectTimeoutMillis), TimeUnit.MILLISECONDS);

        MongoClientProvider.logger.info("Socket Read Timeout Millis:" + makeTimeout(readTimeoutMillis));
        builder.readTimeout(makeTimeout(readTimeoutMillis), TimeUnit.MILLISECONDS);

        if (receiveBufferSize != null) {
            MongoClientProvider.logger.info("Socket receive Buffer Size:" + receiveBufferSize);
            builder.receiveBufferSize(receiveBufferSize);
        }

        if (sendBufferSize != null) {
            MongoClientProvider.logger.info("Socket Send Buffer Size:" + sendBufferSize);
            builder.sendBufferSize(sendBufferSize);
        }

        return this;
    }

    MongoClientConf applyConnectionPoolSettings(ConnectionPoolSettings.Builder builder){
        if (maintenanceFrequencyMillis != null) {
            MongoClientProvider.logger.info("Connection Pool maintenance Frequency Millis:" + maintenanceFrequencyMillis);
            builder.maintenanceFrequency(maintenanceFrequencyMillis, TimeUnit.MILLISECONDS);
        }

        if (maintenanceInitialDelayMillis != null) {
            MongoClientProvider.logger.info("Connection Pool maintenance Initial Delay Millis:" + maintenanceInitialDelayMillis);
            builder.maintenanceInitialDelay(maintenanceInitialDelayMillis, TimeUnit.MILLISECONDS);
        }

        if (maxConnectionIdleTimeMillis != null) {
            MongoClientProvider.logger.info("Connection Pool max Connection Idle Time Millis:" + maxConnectionIdleTimeMillis);
            builder.maxConnectionIdleTime(maxConnectionIdleTimeMillis, TimeUnit.MILLISECONDS);
        }

        if (maxConnectionLifeTimeMillis != null) {
            MongoClientProvider.logger.info("Connection Pool max Connection Life Time Millis:" + maxConnectionLifeTimeMillis);
            builder.maxConnectionLifeTime(maxConnectionLifeTimeMillis, TimeUnit.MILLISECONDS);
        }

        if (maxWaitTimeMillis != null) {
            MongoClientProvider.logger.info("Connection Pool max Wait Time Millis:" + maxWaitTimeMillis);
            builder.maxWaitTime(maxWaitTimeMillis, TimeUnit.MILLISECONDS);
        }

        if (maxSize != null) {
            MongoClientProvider.logger.info("Connection Pool max Size:" + maxSize);
            builder.maxSize(maxSize);
        }

        if (minSize != null) {
            MongoClientProvider.logger.info("Connection Pool min Size:" + minSize);
            builder.minSize(minSize);
        }

        return this;
    }

    MongoClientConf applyServerSettings(ServerSettings.Builder builder) {
        if (heartbeatFrequencyMillis != null) {
            MongoClientProvider.logger.info("Server Heartbeat Frequency Millis:" + heartbeatFrequencyMillis);
            builder.heartbeatFrequency(heartbeatFrequencyMillis, TimeUnit.MILLISECONDS);
        }

        if (minHeartbeatFrequencyMillis != null) {
            MongoClientProvider.logger.info("Server Min Heartbeat Frequency Millis:" + minHeartbeatFrequencyMillis);
            builder.minHeartbeatFrequency(minHeartbeatFrequencyMillis, TimeUnit.MILLISECONDS);
        }

        return this;
    }

    MongoClientConf setCredential(MongoClientSettings.Builder builder, Object secret){
        if (StringUtils.isNotBlank(username)) {
            char[] password;
            if (secret == null){
                password = "".toCharArray();
                MongoClientProvider.logger.warn("No password available for connection to " + url);
            }else
                password = secret.getClass().equals(char[].class) ?
                        (char[]) secret
                        : secret.toString().toCharArray();

            builder.credential(MongoCredential.createCredential(username, database, password));
        }

        return this;
    }
}
