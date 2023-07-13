package org.sv.flexobject.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.*;

import java.io.IOException;
import java.util.*;

public class DockerUtils {

    public enum ContainerState{
        created,
        running,
        restarting,
        exited,
        paused,
        dead
    }

    public static List<Image> getImagesForRepoName(DockerClient dockerClient, String name){
        List<Image> matchedImages = new ArrayList<>();
        List<Image> images = dockerClient.listImagesCmd().exec();
        for (Image image : images) {
            for (String tag : image.getRepoTags()){
                String repoName = tag.split(":")[0];
                if (repoName.equals(name)) {
                    matchedImages.add(image);
                    break;
                }
            }
        }
        return matchedImages;
    }

    public static Optional<Image> getImage(DockerClient dockerClient, String imageName, String tag){
        List<Image> images = dockerClient.listImagesCmd().exec();
        String expectedRepoTag = imageName + ":" + tag;
        for (Image image : images) {
            for (String repoTag : image.getRepoTags()){
                if (repoTag.equals(expectedRepoTag))
                    return Optional.of(image);
            }
        }
        return Optional.empty();
    }

    public static List<Container> getImageContainers(DockerClient dockerClient, Image image){
        List<Container> matchingContainers = new ArrayList<>();
        List<Container> containers = dockerClient.listContainersCmd().withShowAll(true).exec();
        for (Container container : containers){
            if (container.getImageId().equals(image.getId())) {
                matchingContainers.add(container);
            }
        }
        return matchingContainers;
    }
    public static void removeImages(DockerClient dockerClient, List<Image> images){
        for (Image image : images){
            List<Container> containers = getImageContainers(dockerClient, image);
            for (Container container : containers) {
                switch (ContainerState.valueOf(container.getState())) {
                    case running:
                    case restarting:
                    case paused:
                        dockerClient.stopContainerCmd(container.getId()).exec();
                }
                dockerClient.removeContainerCmd(container.getId()).exec();
            }
            for (String tag : image.getRepoTags()) {
                dockerClient.removeImageCmd(tag).exec();
            }
        }
    }

    public static Image checkAndPullImage(DockerClient dockerClient, String imageName, String tag) throws IOException, InterruptedException {
        Optional<Image> image = getImage(dockerClient, imageName, tag);
        if (image.isPresent())
            return image.get();

        try(ResultCallback.Adapter<PullResponseItem> adapter = dockerClient
                .pullImageCmd(imageName)
                .withTag(tag)
                .start()) {
            adapter.awaitCompletion();
        }

        image = getImage(dockerClient, imageName, tag);
        if (image.isPresent())
            return image.get();
        else
            throw new IOException(String.format("Failed to pull image %s:%s", imageName, tag));
    }

    public static Container getContainer(DockerClient dockerClient, String name){
        InspectContainerResponse response  = dockerClient.inspectContainerCmd(name).exec();
        System.out.println(response);
        List<Container> containers = dockerClient.listContainersCmd().withShowAll(true).withIdFilter(Arrays.asList(response.getId())).exec();
        return containers.isEmpty() ? null : containers.get(0);
    }
    public static Container checkCreateContainer(DockerClient dockerClient, Image image, String name, boolean stopIfRunning){
        List<Container> containers = DockerUtils.getImageContainers(dockerClient, image);

        if (!containers.isEmpty()) {
            Container container = containers.get(0);
            if (stopIfRunning) {
                switch (ContainerState.valueOf(container.getState())) {
                    case running:
                    case paused:
                        dockerClient.stopContainerCmd(container.getId()).exec();
                }
            }
            return container;
        } else {
            dockerClient.createContainerCmd(image.getId()).withName(name).exec();
            return DockerUtils.getImageContainers(dockerClient, image).get(0);
        }
    }

    public static List<PortBinding> localhostPortBindings(Map<Integer, Integer> ports){
        return portBindings("0.0.0.0", ports);
    }

    public static List<PortBinding> localhostPortBindings(List<Integer> ports){
        return portBindings("0.0.0.0", ports);
    }

    public static List<PortBinding> portBindings(String hostIp, Map<Integer, Integer> ports){
        List<PortBinding> bindings = new ArrayList<>(ports.size());
        for (Map.Entry<Integer, Integer> entry : ports.entrySet()){
            bindings.add(new PortBinding(new Ports.Binding(hostIp, Integer.toString(entry.getKey())), new ExposedPort(entry.getValue())));
        }
        return bindings;
    }

    public static List<PortBinding> portBindings(String hostIp, List<Integer> ports){
        List<PortBinding> bindings = new ArrayList<>();
        for (Integer port : ports){
            bindings.add(new PortBinding(new Ports.Binding(hostIp, Integer.toString(port)), new ExposedPort(port)));
        }
        return bindings;
    }

    public static List<ExposedPort> exposedTcpPorts(Collection<Integer> ports){
        List<ExposedPort> exposedPorts = new ArrayList<>();
        for (Integer port : ports){
            exposedPorts.add(ExposedPort.tcp(port));
        }
        return exposedPorts;
    }

    public static void startRestartContainer(DockerClient dockerClient, Container container) {
        switch (ContainerState.valueOf(container.getState())){
            case running:
            case paused:
                dockerClient.stopContainerCmd(container.getId()).exec();
                break;
            case restarting:
                return;
        }
        dockerClient.startContainerCmd(container.getId()).exec();
    }

    public static ContainerState waitContainerRunning(DockerClient dockerClient, Container container) {
        while (true) {
            InspectContainerResponse response = dockerClient.inspectContainerCmd(container.getId()).exec();
            switch (ContainerState.valueOf(response.getState().getStatus())) {
                case dead:
                    return ContainerState.dead;
                case exited:
                    return ContainerState.exited;
                case running:
                    return ContainerState.running;
            }
        }
    }

}
