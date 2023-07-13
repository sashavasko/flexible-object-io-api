package org.sv.flexobject.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.Image;
import com.github.dockerjava.api.model.Info;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class DockerClientProviderTest {

    static DockerClient dockerClient;
    static final String IMAGE_NAME = "hello-world";

    @BeforeClass
    public static void beforeClass() throws Exception {
        dockerClient = DockerClientProvider.getDefault();
        List<Image> oldImages = DockerUtils.getImagesForRepoName(dockerClient, IMAGE_NAME);
        if (!oldImages.isEmpty())
            DockerUtils.removeImages(dockerClient, oldImages);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        dockerClient.close();
    }

    @Test
    public void testPing() throws Exception {
        dockerClient.pingCmd().exec();
        Info info = dockerClient.infoCmd().exec();

        assertNotNull(info);

        assertEquals("x86_64", info.getArchitecture());
        System.out.println(info);
    }

    @Test
    public void pullImage() throws IOException, InterruptedException {
        Image image = DockerUtils.checkAndPullImage(dockerClient, IMAGE_NAME, "linux");
        assertNotNull(image);
        assertEquals(-1L, (long)image.getContainers());
    }

    @Test
    public void createContainer() throws IOException, InterruptedException {
        Image image = DockerUtils.checkAndPullImage(dockerClient, IMAGE_NAME, "linux");
        assertNotNull(image);
        Container imageContainer = DockerUtils.checkCreateContainer(dockerClient, image, "TestContainer", true);
        assertNotNull(imageContainer);

        Container byName = DockerUtils.getContainer(dockerClient, "TestContainer");
        assertNotNull(byName);
    }

    @Test
    public void startContainer() throws IOException, InterruptedException {
        Image image = DockerUtils.checkAndPullImage(dockerClient, IMAGE_NAME, "linux");
        assertNotNull(image);
        Container imageContainer = DockerUtils.checkCreateContainer(dockerClient, image, "TestContainer", true);
        assertNotNull(imageContainer);

        dockerClient.startContainerCmd(imageContainer.getId()).exec();

        Thread.sleep(1000);
        InspectContainerResponse response = dockerClient.inspectContainerCmd(imageContainer.getId()).exec();
        assertEquals(DockerUtils.ContainerState.exited, DockerUtils.ContainerState.valueOf(response.getState().getStatus()));
    }



    @Test
    public void listContainers() {
        List<Container> containers = dockerClient.listContainersCmd().withShowAll(true).exec();

        for (Container container : containers){
            System.out.println(container);
        }
    }

    @Test
    public void listImages(){
        List<Image> images = dockerClient.listImagesCmd().exec();
        for (Image image : images) {
            for (String tag : image.getRepoTags()){
                String imageName = tag.split(":")[0];
                System.out.println(imageName);
                break;
            }
        }
    }

}