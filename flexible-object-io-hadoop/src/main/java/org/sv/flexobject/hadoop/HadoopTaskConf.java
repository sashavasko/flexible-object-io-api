package org.sv.flexobject.hadoop;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.Tool;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesProvider;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;
import org.sv.flexobject.hadoop.properties.HadoopSecretProvider;
import org.sv.flexobject.mongo.MongoClientProvider;
import org.sv.flexobject.properties.Namespace;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ValueType;
import org.sv.flexobject.sql.providers.UnPooledConnectionProvider;
import org.sv.flexobject.util.InstanceFactory;

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
    private Class<? extends Tool> toolClass;
    private Class<? extends HadoopPropertiesWrapper> confClass;

    @Override
    protected String getSubNamespace() {
        return SUBNAMESPACE;
    }

    public HadoopTaskConf() {
        super(SUBNAMESPACE);
    }

    public HadoopTaskConf(Namespace parent, String subNamespace) {
        super(parent, subNamespace);
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


    public Class<? extends Tool> getToolClass() {
        return toolClass;
    }

    public <T extends Tool> T getTool() {
        Tool tool = InstanceFactory.get(getToolClass());
        return (T)tool.getClass().cast(tool);
    }

    public Class<? extends HadoopPropertiesWrapper> getConfClass(){
        return confClass;
    }

    public <T extends HadoopPropertiesWrapper> T instantiateConfig(){
        HadoopPropertiesWrapper conf = InstanceFactory.get(getConfClass());
        return (T)conf.getClass().cast(conf);
    }
}
