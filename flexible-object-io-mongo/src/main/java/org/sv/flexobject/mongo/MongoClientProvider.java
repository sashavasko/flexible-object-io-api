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

    Logger logger = LogManager.getLogger(MongoClientProvider.class);

    @Override
    public AutoCloseable getConnection(String name, Properties connectionProperties, Object secret) throws Exception {
        String url = connectionProperties.getProperty("url");

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

    @Override
    public Iterable<Class<? extends AutoCloseable>> listConnectionTypes() {
        return Arrays.asList(MongoClient.class);
    }
}
