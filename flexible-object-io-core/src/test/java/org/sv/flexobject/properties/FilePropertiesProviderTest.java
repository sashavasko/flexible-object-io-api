package org.sv.flexobject.properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.connections.ConnectionManager;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class FilePropertiesProviderTest {

    FilePropertiesProvider provider;

    @Before
    public void setUp() throws Exception {
        provider = new FilePropertiesProvider("src/test/props/dir1", "src/test/props/dir2", "src/test/props/dir3", "src/test/props/dir4");
    }

    @Test
    public void realFiles() {
        Properties props;
        props = provider.getProperties("conn1", ConnectionManager.DeploymentLevel.alpha, "someenv");
        assertEquals("foo", props.getProperty("property1"));
        assertEquals("bar", props.getProperty("property2"));

        props = provider.getProperties("conn2", ConnectionManager.DeploymentLevel.alpha, "env1");
        assertEquals("foo", props.getProperty("property1"));
        assertEquals("bar", props.getProperty("property2"));

        props = provider.getProperties("conn3",
                ConnectionManager.DeploymentLevel.beta,
                "env2");
        assertEquals("foo", props.getProperty("property1"));
        assertEquals("bar", props.getProperty("property2"));

        props = provider.getProperties("conn4",
                ConnectionManager.DeploymentLevel.alpha,
                "env3");
        assertEquals("fooenv3", props.getProperty("property1"));
        assertEquals("barenv3", props.getProperty("property2"));

        props = provider.getProperties("conn4",
                ConnectionManager.DeploymentLevel.alpha,
                "env4");
        assertEquals("fooenv4", props.getProperty("property1"));
        assertEquals("barenv4", props.getProperty("property2"));

        props = provider.getProperties("conn5",
                ConnectionManager.DeploymentLevel.alpha,
                "env1");
        assertEquals("fooenv1", props.getProperty("property1"));
        assertEquals("barenv1", props.getProperty("property2"));

        props = provider.getProperties("conn6",
                ConnectionManager.DeploymentLevel.alpha,
                "env5");
        assertEquals("fooconn6", props.getProperty("property1"));
        assertEquals("barconn6", props.getProperty("property2"));

        props = provider.getProperties("conn7",
                ConnectionManager.DeploymentLevel.alpha,
                "env5");
        assertEquals("fooconn7", props.getProperty("property1"));
        assertEquals("barconn7", props.getProperty("property2"));

        props = provider.getProperties("conn8",
                ConnectionManager.DeploymentLevel.alpha,
                "env2");
        assertEquals("fooenv2", props.getProperty("property1"));
        assertEquals("barenv2", props.getProperty("property2"));

        props = provider.getProperties("conn9",
                ConnectionManager.DeploymentLevel.beta,
                "env7");
        assertEquals("fooenv7", props.getProperty("property1"));
        assertEquals("barenv7", props.getProperty("property2"));

        props = provider.getProperties("conn9",
                ConnectionManager.DeploymentLevel.beta,
                "env8");
        assertEquals("fooenv8", props.getProperty("property1"));
        assertEquals("barenv8", props.getProperty("property2"));

        props = provider.getProperties("conn10",
                ConnectionManager.DeploymentLevel.beta,
                "env6");
        assertEquals("fooconn10", props.getProperty("property1"));
        assertEquals("barconn10", props.getProperty("property2"));

        props = provider.getProperties("conn11",
                ConnectionManager.DeploymentLevel.beta,
                "env6");
        assertEquals("fooconn11", props.getProperty("property1"));
        assertEquals("barconn11", props.getProperty("property2"));

        props = provider.getProperties("conn12", ConnectionManager.DeploymentLevel.alpha, "env1");
        assertEquals("fooconn12", props.getProperty("property1"));
        assertEquals("barconn12", props.getProperty("property2"));

//        props = provider.getProperties("conn5", ConnectionManager.DeploymentLevel.alpha, "env1");
//        assertEquals("fooenv1", props.getProperty("property1"));
//        assertEquals("barenv1", props.getProperty("property2"));
//
//        props = provider.getProperties("conn8", ConnectionManager.DeploymentLevel.alpha, "env2");
//        assertEquals("fooenv2", props.getProperty("property1"));
//        assertEquals("barenv2", props.getProperty("property2"));
//
//        props = provider.getProperties("conn4", ConnectionManager.DeploymentLevel.alpha, "env3");
//        assertEquals("fooenv3", props.getProperty("property1"));
//        assertEquals("barenv3", props.getProperty("property2"));
    }
}