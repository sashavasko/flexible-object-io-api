package org.sv.flexobject.mongo;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.sv.flexobject.connections.ConnectionProvider;
import org.sv.flexobject.mongo.codecs.SqlDateCodec;
import org.sv.flexobject.mongo.codecs.TimestampCodec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class MongoClientProvider implements ConnectionProvider {

    public static final Logger logger = LogManager.getLogger(MongoClientProvider.class);

    public static final String MONGODB_PREFIX = "mongodb://";
    public static final String MONGODB_SRV_PREFIX = "mongodb+srv://";

    @Override
    public AutoCloseable getConnection(String name, Properties connectionProperties, Object secret) throws Exception {
        String url = connectionProperties.getProperty("url");

        if (connectionProperties.containsKey("hosts")){
            url = replaceHosts(url, connectionProperties.getProperty("hosts"));
        }

        ConnectionString connectionString = new ConnectionString(url);

        MongoClientSettings.Builder builder = MongoClientSettings.builder();

        builder.applyConnectionString(connectionString);

        List<TagSet> tagsList = new ArrayList<>();
        String tags = connectionProperties.getProperty("tags");
        if (StringUtils.isNotBlank(tags)) {
            String[] tagPairs = tags.split(",");
            for (String tagPair : tagPairs) {
                String[] tagValues = tagPair.split(":");
                if (tagValues.length == 2){
                    Tag tag = new Tag(tagValues[0], tagValues[1]);
                    tagsList.add(new TagSet(tag));
                }
            }
        }

        ReadPreference readPreference = ReadPreference.secondaryPreferred(tagsList);
        ReadPreference readPreferenceFromUrl = connectionString.getReadPreference();
        String readPreferenceFromSetting = connectionProperties.getProperty("readPreference");
        if (StringUtils.isNotBlank(readPreferenceFromSetting)){
            readPreference = ReadPreference.valueOf(readPreferenceFromSetting, tagsList);
        } else if (readPreferenceFromUrl != null)
            readPreference = readPreferenceFromUrl;

        builder.readPreference(readPreference);

        String compressors = connectionProperties.getProperty("compressorList");
        if (StringUtils.isNotBlank(compressors)) {
            List<MongoCompressor> compressorList = new ArrayList<>();
            for(String compressor : compressors.split(",")){
                switch(compressor.toLowerCase()){
                    case "snappy" : compressorList.add(MongoCompressor.createSnappyCompressor()); break;
                    case "zlib" : compressorList.add(MongoCompressor.createZlibCompressor()); break;
                    case "zstd" : compressorList.add(MongoCompressor.createZstdCompressor()); break;
                }
            }
            builder.compressorList(compressorList);
        }

        String timeout = connectionProperties.getProperty("timeout", "60000");
        int connectTimeout = Integer.valueOf(connectionProperties.getProperty("connectTimeout", timeout));
        int readTimeout = Integer.valueOf(connectionProperties.getProperty("readTimeout", timeout));
        builder.applyToSocketSettings(b ->
                b.connectTimeout(connectTimeout, TimeUnit.MILLISECONDS)
                        .readTimeout(readTimeout, TimeUnit.MILLISECONDS));

        int maxConnections = Integer.valueOf(connectionProperties.getProperty("maxConnections", "1"));
        int maxWaitTime = Integer.valueOf(connectionProperties.getProperty("maxWaitTime", timeout));
        builder.applyToConnectionPoolSettings(b ->
                b.maxWaitTime(maxWaitTime, TimeUnit.MILLISECONDS)
                        .maxSize(maxConnections));

        String userName = connectionProperties.getProperty("username", connectionString.getUsername());
        if (StringUtils.isNotBlank(userName)) {
            String databaseName = connectionProperties.getProperty("database", connectionString.getDatabase());
            char[] password;
            if (secret == null){
                password = "".toCharArray();
                logger.warn("No password available for connection to " + url);
            }else
                password = secret.getClass().equals(char[].class) ?
                        (char[]) secret
                        : secret.toString().toCharArray();

            builder.credential(MongoCredential.createCredential(userName, databaseName, password));
        }
//      The following should happen here : builder.applyConnectionString(connectionString);
//
//        List<String> mongoHosts = connectionString.getHosts();
//        List<ServerAddress> servers = new ArrayList<>();
//        for (String host : mongoHosts) {
//            servers.add(new ServerAddress(host));
//        }
//        builder.applyToClusterSettings(b -> b.hosts(servers));

        CodecRegistry pojoCodecRegistry = fromProviders(PojoCodecProvider.builder().automatic(true).build());
        CodecRegistry codecRegistry = fromRegistries(
                CodecRegistries.fromCodecs(new TimestampCodec(),new SqlDateCodec()),
                MongoClientSettings.getDefaultCodecRegistry(),
                pojoCodecRegistry);
        builder.codecRegistry(codecRegistry);

        MongoClient mongo = MongoClients.create(builder.build());
        logger.info("connected to Mongo");

        return mongo;
    }

    protected String replaceHosts(String url, String hosts) {
        String hostsPrefix = parseUrlHostsPrefix(url);
        String hostsSuffix = parseUrlHostsSuffix(hostsPrefix.length(), url);

        return hostsPrefix + hosts + hostsSuffix;
    }

    private String parseUrlHostsSuffix(int prefixLength, String url) {
        String suffix = url.substring(prefixLength);
        int idx = suffix.indexOf("/");
        return idx == -1 ? "" : suffix.substring(idx);
    }

    private String parseUrlHostsPrefix(String url) {
        String unprocessedConnectionString;
        StringBuilder sb = new StringBuilder();
        if (url.startsWith(MONGODB_SRV_PREFIX)) {
            unprocessedConnectionString = url.substring(MONGODB_SRV_PREFIX.length());
            sb.append(MONGODB_SRV_PREFIX);
        } else {
            unprocessedConnectionString = url.substring(MONGODB_PREFIX.length());
            sb.append(MONGODB_PREFIX);
        }

        // Stolen from mongodb driver sources :
        // Split out the user and host information
        String userAndHostInformation;
        int idx = unprocessedConnectionString.indexOf("/");
        if (idx == -1) {
            if (unprocessedConnectionString.contains("?")) {
                throw new IllegalArgumentException("The connection string contains options without trailing slash");
            }
            userAndHostInformation = unprocessedConnectionString;
        } else {
            userAndHostInformation = unprocessedConnectionString.substring(0, idx);
        }

        idx = userAndHostInformation.lastIndexOf("@");
        if (idx > 0) {
            sb.append(userAndHostInformation, 0, idx+1);
        } else if (idx == 0) {
            throw new IllegalArgumentException("The connection string contains an at-sign (@) without a user name");
        }

        return sb.toString();
    }

    @Override
    public Iterable<Class<? extends AutoCloseable>> listConnectionTypes() {
        return Arrays.asList(MongoClient.class);
    }
}
