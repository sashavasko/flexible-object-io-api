package org.sv.flexobject.connections;

import org.sv.flexobject.util.ConsumerWithException;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;
import java.util.*;

public class ConnectionManager {
    // TODO add extensive Loggin for providers registration/unregistration

    private static ConnectionManager instance = null;
    private final Object lock = new Object();

    public enum DeploymentLevel{
        alpha,
        beta,
        prod
    }

    private DeploymentLevel deploymentLevel = DeploymentLevel.alpha;
    private String environment = null;
    private Map<Class<? extends AutoCloseable>, ConnectionProvider> connectionProviders = new HashMap<>();
    private LinkedList<PropertiesProvider> propertiesProviders = new LinkedList<>();
    private LinkedList<SecretProvider> secretProviders = new LinkedList<>();

    private ConnectionManager(){
        environment = System.getProperty("os.name");
    }

    public static ConnectionManager getInstance(){
        if (instance == null)
            instance = new ConnectionManager();
        return instance;
    }

    /**
     *
     * @param deploymentLevel - deployment level to pass to properties and secret providers (alpha, beta or prod). DEFAULT: alpha
     */
    public ConnectionManager setDeploymentLevel(DeploymentLevel deploymentLevel) {
        this.deploymentLevel = deploymentLevel;
        return this;
    }

    /**
     *
     * @param environment - optional free-form environment identifier to be passed to properties and secret providers, such as "batch", "linux", "aws", etc.
     */

    public ConnectionManager setEnvironment(String environment) {
        this.environment = environment;
        return this;
    }

    public DeploymentLevel getDeploymentLevel() {
        return deploymentLevel;
    }

    public String getEnvironment() {
        return environment;
    }

    public static AutoCloseable getConnection(Class<? extends AutoCloseable> connectionType, String connectionName) throws Exception {
        return getInstance().getConnectionImpl(connectionType, connectionName);
    }

    /*
     * This method can be used to setup ConnectionManager from the list of provider classes configured at runtime,
     * such as command line, system property, configuration file etc.
     */
    public static ConnectionManager addProviders(Class ... providerClasses){
        return getInstance().addProvidersImpl(Arrays.asList(providerClasses));
    }

    public static ConnectionManager addProviders(Iterable<Class> providerClasses){
        return getInstance().addProvidersImpl(providerClasses);
    }

    public static void forEachProvider(ConsumerWithException<Provider, Exception> consumer) throws Exception {
        getInstance().forEachProviderImpl(null, consumer);
    }

    public static void forEachProvider(Class implementsClass, ConsumerWithException<Provider, Exception> consumer) throws Exception {
        getInstance().forEachProviderImpl(implementsClass, consumer);
    }

    public ConnectionManager registerProvider(ConnectionProvider provider, Class<? extends AutoCloseable> connectionType1, Class<? extends AutoCloseable> ... connectionTypes){
        synchronized (lock) {
            registerProvider(provider, connectionType1);
        }
        return registerProvider(provider, Arrays.asList(connectionTypes));
    }

    public ConnectionManager registerProvider(ConnectionProvider provider){
        Iterable<Class<? extends AutoCloseable>> connectionTypes = provider.listConnectionTypes();
        return registerProvider(provider, connectionTypes);
    }

    public ConnectionManager registerProvider(ConnectionProvider provider, Iterable<Class<? extends AutoCloseable>> connectionTypes){
        synchronized (lock) {
            for (Class<? extends AutoCloseable> connectionType : connectionTypes) {
                registerProvider(provider, connectionType);
            }
        }
        return this;
    }

    public ConnectionManager registerProvider(Class<? extends ConnectionProvider> providerClass, Class<? extends AutoCloseable> connectionType1, Class<? extends AutoCloseable> ... connectionTypes){
        return registerProvider(InstanceFactory.get(providerClass), connectionType1, connectionTypes);
    }

    public ConnectionManager registerProvider(Class<? extends ConnectionProvider> providerClass){
        return registerProvider(InstanceFactory.get(providerClass));
    }

    public ConnectionManager registerProvider(Class<? extends ConnectionProvider> providerClass, Iterable<Class<? extends AutoCloseable>> connectionTypes){
        return registerProvider(InstanceFactory.get(providerClass), connectionTypes);
    }

    /*
     *   This way to unregister connection provider will automatically close any provider implementing AutoCloseable
     */
    public ConnectionManager unregisterProvider(Class<? extends ConnectionProvider> providerClass){
        synchronized (lock) {
            return unregisterProviderClassForAllConnectionTypes(providerClass);
        }
    }

    public ConnectionManager unregisterProvider(ConnectionProvider provider){
        synchronized (lock) {
            return unregisterProvidersForAllConnectionTypes(provider);
        }
    }

    public ConnectionManager registerSecretProvider(SecretProvider provider){
        unregisterSecretProvider(provider);
        secretProviders.add(provider);
        return this;
    }

    public ConnectionManager registerSecretProvider(Class<? extends SecretProvider> providerClass){
        unregisterSecretProvider(providerClass);
        secretProviders.add(InstanceFactory.get(providerClass));
        return this;
    }

    public ConnectionManager unregisterSecretProvider(SecretProvider provider){
        synchronized (lock) {
            secretProviders.remove(provider);
            return this;
        }
    }

    public ConnectionManager unregisterSecretProvider(Class<? extends SecretProvider> providerClass){
        synchronized (lock) {
            return unregisterPropertiesProvider(providerClass, secretProviders);
        }
    }

    public ConnectionManager registerPropertiesProvider(PropertiesProvider provider){
        unregisterPropertiesProvider(provider);
        propertiesProviders.add(provider);
        return this;
    }

    public ConnectionManager unregisterPropertiesProvider(PropertiesProvider provider){
        synchronized (lock) {
            propertiesProviders.remove(provider);
            return this;
        }
    }

    public ConnectionManager unregisterPropertiesProvider(Class<? extends PropertiesProvider> providerClass){
        synchronized (lock) {
            return unregisterPropertiesProvider(providerClass, propertiesProviders);
        }
    }

    public ConnectionManager clearProviders(){
        synchronized (lock) {
            Object[] keys = connectionProviders.keySet().toArray();
            for (Object key : keys) {
                removeConnectionType((Class<? extends AutoCloseable>) key);
            }
            return this;
        }
    }

    public ConnectionManager clearPropertiesProviders(){
        propertiesProviders.clear();
        return this;
    }

    public ConnectionManager clearSecretProviders(){
        secretProviders.clear();
        return this;
    }

    public ConnectionManager clearAll(){
        clearProviders();
        clearPropertiesProviders();
        clearSecretProviders();
        return this;
    }

    protected ConnectionManager addProvidersImpl(Iterable<Class> providerClasses){
        synchronized (lock) {
            for (Class clazz : providerClasses) {

                if (ConnectionProvider.class.isAssignableFrom(clazz))
                    registerProvider(clazz);

                if (PropertiesProvider.class.isAssignableFrom(clazz)) {
                    unregisterPropertiesProvider(clazz, propertiesProviders);
                    propertiesProviders.add((PropertiesProvider) InstanceFactory.get(clazz));
                }

                if (SecretProvider.class.isAssignableFrom(clazz)) {
                    unregisterPropertiesProvider(clazz, secretProviders);
                    secretProviders.add((SecretProvider) InstanceFactory.get(clazz));
                }
            }
            return this;
        }
    }

    protected AutoCloseable getConnectionImpl(Class<? extends AutoCloseable> connectionType, String connectionName) throws Exception {
        synchronized (lock) {
            ConnectionProvider connectionProvider = connectionProviders.get(connectionType);
            if (connectionProvider == null) {
                throw new IOException("Unknown connection provider for " + connectionType + " named " + connectionName);
            }

            Properties connectionProperties = null;
            Object secret = null;
            for (PropertiesProvider provider : propertiesProviders) {
                connectionProperties = provider.getProperties(connectionName, deploymentLevel, environment);
                if (connectionProperties != null && !connectionProperties.isEmpty())
                    break;
            }

            for (SecretProvider provider : secretProviders) {
                secret = provider.getSecret(connectionName, deploymentLevel, environment);
                if (secret != null)
                    break;
            }

            return connectionProvider.getConnection(connectionName, connectionProperties, secret);
        }
    }

    protected void forEachProviderImpl(Class implementsClass, ConsumerWithException<Provider, Exception> consumer) throws Exception {
        for (ConnectionProvider provider : connectionProviders.values()) {
            consumeProvider(provider, consumer, implementsClass);
        }
        for (PropertiesProvider provider : propertiesProviders) {
            consumeProvider(provider, consumer, implementsClass);
        }
        for (SecretProvider provider : secretProviders) {
            consumeProvider(provider, consumer, implementsClass);
        }
    }

    /*
     * Not Thread Safe!
     */
    protected ConnectionManager unregisterPropertiesProvider(Class providerClass, LinkedList<? extends PropertiesProvider> list) {
        for (PropertiesProvider provider : list) {
            if (provider.getClass().equals(providerClass)) {
                list.remove(provider);
                // there could me multiple instances make sure we remove all
                return unregisterPropertiesProvider(providerClass, list);
            }
        }
        return this;
    }

    protected void consumeProvider(Provider provider, ConsumerWithException<Provider, Exception> consumer, Class implementsClass) throws Exception {
        if (implementsClass == null || implementsClass.isAssignableFrom(provider.getClass()))
            consumer.accept(provider);
    }

    private ConnectionManager unregisterProviderClassForAllConnectionTypes(Class<? extends ConnectionProvider> providerClass) {
        for (Map.Entry<Class<? extends AutoCloseable>, ConnectionProvider> entry : connectionProviders.entrySet()) {
            if (entry.getValue().getClass().equals(providerClass)) {
                removeConnectionType(entry.getKey());
                // avoid ConcurrentModificationException

                return unregisterProviderClassForAllConnectionTypes(providerClass);
            }
        }
        return this;
    }

    private ConnectionManager unregisterProvidersForAllConnectionTypes(ConnectionProvider provider) {
        for (Map.Entry<Class<? extends AutoCloseable>, ConnectionProvider> entry : connectionProviders.entrySet()) {
            if (entry.getValue() == provider) {
                removeConnectionType(entry.getKey());
                // avoid ConcurrentModificationException
                return unregisterProvidersForAllConnectionTypes(provider);
            }
        }
        return this;
    }

    private void removeConnectionType(Class<? extends AutoCloseable> connectionType){
        ConnectionProvider old = connectionProviders.get(connectionType);
        if (old != null && old instanceof AutoCloseable){
            try {
                ((AutoCloseable) old).close();
            } catch (Exception e) {
            }
        }
        connectionProviders.remove(connectionType);
    }

    private void registerProvider(ConnectionProvider provider, Class<? extends AutoCloseable> connectionType){
        removeConnectionType(connectionType);
        connectionProviders.put(connectionType, provider);
    }
}
