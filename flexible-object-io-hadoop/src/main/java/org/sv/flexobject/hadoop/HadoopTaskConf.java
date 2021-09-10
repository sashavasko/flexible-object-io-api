package org.sv.flexobject.hadoop;

import org.apache.commons.lang3.StringUtils;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesProvider;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;
import org.sv.flexobject.hadoop.properties.HadoopSecretProvider;
import org.sv.flexobject.mongo.MongoClientProvider;
import org.sv.flexobject.properties.Namespace;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ValueType;
import org.sv.flexobject.sql.providers.UnPooledConnectionProvider;

import java.util.Arrays;
import java.util.List;

public class HadoopTaskConf extends HadoopPropertiesWrapper<HadoopTaskConf> {

    public static final String SUBNAMESPACE = "hadoop";
    public static final List<Class> DEFAULT_PROVIDERS = Arrays.asList(
            HadoopSecretProvider.class,
            HadoopPropertiesProvider.class,
            UnPooledConnectionProvider.class,
            MongoClientProvider.class
    );

    public static final String DEFAULT_ENVIRONMENT = "hadoop";
    public static final ConnectionManager.DeploymentLevel DEFAULT_DEPLOYMENT_LEVEL = ConnectionManager.DeploymentLevel.beta;

    private String dbEnvironment;
    private String deploymentLevel;
    @ValueType(type= DataTypes.classObject)
    private List<Class> connectionManagerProviders;
    private String connectionManagerEnvironment;

    public HadoopTaskConf() {
        super(SUBNAMESPACE);
    }

    @Override
    public HadoopTaskConf setDefaults() {
        return this;
    }

    public HadoopTaskConf(Namespace parent) {
        super(parent, SUBNAMESPACE);
    }

    public ConnectionManager.DeploymentLevel getDeploymentLevel() {
        return StringUtils.isNotBlank(deploymentLevel) ? ConnectionManager.DeploymentLevel.valueOf(deploymentLevel)
                : StringUtils.isNotBlank(dbEnvironment) ? ConnectionManager.DeploymentLevel.valueOf(dbEnvironment)
                : DEFAULT_DEPLOYMENT_LEVEL;
    }

    public Iterable<Class> getConnectionManagerProviders() {
        return connectionManagerProviders != null && connectionManagerProviders.size() > 0 ? connectionManagerProviders
                : DEFAULT_PROVIDERS;
    }

    public String getConnectionManagerEnvironment() {
        return StringUtils.isNotBlank(connectionManagerEnvironment) ? connectionManagerEnvironment
                : DEFAULT_ENVIRONMENT;
    }

    public void setDeploymentLevel(ConnectionManager.DeploymentLevel deploymentLevel) {
        this.deploymentLevel = deploymentLevel.name();
    }

    public void setConnectionManagerEnvironment(String connectionManagerEnvironment) {
        this.connectionManagerEnvironment = connectionManagerEnvironment;
    }

    public void setConnectionManagerProviders(List<Class> connectionManagerProviders) {
        this.connectionManagerProviders = connectionManagerProviders;
    }
}
