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
import org.sv.flexobject.util.InstanceFactory;

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
    public static final String HOSTS_OVERRIDE = "hosts";

    @Override
    public AutoCloseable getConnection(String name, Properties connectionProperties, Object secret) throws Exception {

        MongoClientConf conf = InstanceFactory.get(MongoClientConf.class).from(connectionProperties);

        MongoClientSettings.Builder builder = conf.makeClientSettingsBuilder();

        conf.applyCompressorList(builder);
        builder.applyToClusterSettings(b->conf.applyClusterSettings(b));
        builder.applyToSocketSettings(b->conf.applySocketSettings(b));
        builder.applyToConnectionPoolSettings(b->conf.applyConnectionPoolSettings(b));
        builder.applyToServerSettings(b->conf.applyServerSettings(b));

        conf.setCredential(builder, secret);

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
