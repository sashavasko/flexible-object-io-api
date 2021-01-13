package org.sv.flexobject.mongo;

import com.mongodb.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sv.flexobject.connections.ConnectionProvider;
import org.sv.flexobject.util.InstanceFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class MongoClientProvider implements ConnectionProvider {

    Logger logger = LogManager.getLogger(MongoClientProvider.class);

    @Override
    public AutoCloseable getConnection(String name, Properties connectionProperties, Object secret) throws Exception {
        MongoClientURI uri = new MongoClientURI(connectionProperties.getProperty("url"));

        MongoClientOptions.Builder builder = InstanceFactory.get(MongoClientOptions.Builder.class);
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
        int maxConnections = 1;
        int timeout = 60000;
        String databaseName = connectionProperties.getProperty("database");
        String userName = connectionProperties.getProperty("username");
        String readPreference = connectionProperties.getProperty("readPreference");

        char[] password = secret.getClass().equals(char[].class) ?
                (char[])secret
                : secret.toString().toCharArray();

        builder.connectionsPerHost(maxConnections)
                .connectTimeout(timeout)
                .maxWaitTime(timeout)
                .socketTimeout(timeout)
                .readPreference(ReadPreference.valueOf(readPreference, tagsList));

        MongoCredential credential = MongoCredential.createCredential(userName, databaseName, password);
        List<String> mongoHosts = uri.getHosts();
        List<ServerAddress> servers = new ArrayList<>();
        for (String host : mongoHosts) {
            servers.add(new ServerAddress(host));
        }
        MongoClient mongo = new MongoClient(servers, credential, builder.build());
        logger.info("connected to Mongo");

        return mongo;
    }

    @Override
    public Iterable<Class<? extends AutoCloseable>> listConnectionTypes() {
        return Arrays.asList(MongoClient.class);
    }
}
