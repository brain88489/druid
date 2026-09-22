/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.testing.embedded.k8s;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.TestFolder;
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.apache.druid.testing.embedded.docker.DruidContainerResource;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.utils.ITRetryUtil;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * {@link TestcontainerResource} to run a K3s cluster which can launch Druid pods.
 */
public class K3sClusterResource extends TestcontainerResource<K3sContainer>
{
  private static final Logger log = new Logger(K3sClusterResource.class);

  private static final String K3S_IMAGE_NAME = "rancher/k3s:v1.35.0-k3s1";

  public static final String DRUID_NAMESPACE = "druid";
  private static final String NAMESPACE_MANIFEST = "manifests/druid-namespace.yaml";
  private static final String DEFAULT_SERVICE_MANIFEST = "manifests/druid-service.yaml";

  private static final String COMMON_CONFIG_MAP = "druid-common-props";
  private static final String SERVICE_CONFIG_MAP = "druid-%s-props";
  private static final int POD_LOG_TAIL_LINES = 100;
  private static final Set<String> POD_FAILURE_REASONS = Set.of(
      "CrashLoopBackOff",
      "CreateContainerConfigError",
      "CreateContainerError",
      "ErrImagePull",
      "ImagePullBackOff",
      "InvalidImageName",
      "RunContainerError"
  );

  public static final long POD_READY_TIMEOUT_SECONDS = 300;

  private final List<File> manifestFiles = new ArrayList<>();
  private final List<K3sDruidService> services = new ArrayList<>();

  private KubernetesClient client;
  private String druidImageName;
  private String druidManifestTemplate = DEFAULT_SERVICE_MANIFEST;
  private File containerLogsDirectory;

  private final Closer closer = Closer.create();

  public K3sClusterResource()
  {
    // Add the namespace manifest
    addManifestResource(NAMESPACE_MANIFEST);
  }

  public K3sClusterResource addManifestResource(String manifestResourceName)
  {
    manifestFiles.add(Resources.getFileForResource(manifestResourceName));
    return this;
  }

  public K3sClusterResource addService(K3sDruidService service)
  {
    services.add(service);
    return this;
  }

  public K3sClusterResource usingDruidImage(String druidImageName)
  {
    this.druidImageName = druidImageName;
    return this;
  }

  /**
   * Uses the Docker test image specified by the system property
   * {@link DruidContainerResource#PROPERTY_TEST_IMAGE} for the Druid pods.
   */
  public K3sClusterResource usingDruidTestImage()
  {
    return usingDruidImage(DruidContainerResource.getTestDruidImageName());
  }

  /**
   * Uses the given resource template to create manifests for Druid services.
   */
  public K3sClusterResource usingDruidManifestTemplate(String resourceName)
  {
    this.druidManifestTemplate = resourceName;
    return this;
  }

  @Override
  public void beforeStart(EmbeddedDruidCluster cluster)
  {
    containerLogsDirectory = new File(
        new File("druid-container-logs", cluster.getTestClassName()),
        "k3s"
    );
  }

  @Override
  protected K3sContainer createContainer()
  {
    Objects.requireNonNull(druidImageName, "No Druid image specified");
    final K3sContainer container = new K3sContainer(DockerImageName.parse(K3S_IMAGE_NAME));

    final List<String> portBindings = new ArrayList<>();
    for (K3sDruidService service : services) {
      for (int port : service.getCommand().getExposedPorts()) {
        container.addExposedPorts(port);

        // Bind the ports statically (rather than using a mapped port) so that this
        // Druid service is discoverable with the Druid service discovery
        portBindings.add(port + ":" + port);
      }
      int servicePort = service.getServicePort();
      container.addExposedPorts(servicePort);
      portBindings.add(servicePort + ":" + servicePort);
    }

    container.setPortBindings(portBindings);
    return container;
  }

  @Override
  public void onStarted(EmbeddedDruidCluster cluster)
  {
    initKubernetesClient();
    loadLocalDockerImageIntoContainer(druidImageName, cluster.getTestFolder());
    manifestFiles.forEach(this::applyManifest);
    initializeDruidServices(cluster);
    waitUntilPodsAreRunning(DRUID_NAMESPACE);
    waitUntilServicesAreHealthy();
  }

  protected void initializeDruidServices(EmbeddedDruidCluster cluster)
  {
    createConfigMapForCommonProperties(cluster);
    for (K3sDruidService druidService : services) {
      final String serviceConfigMap = StringUtils.format(SERVICE_CONFIG_MAP, druidService.getName());
      applyConfigMap(
          newConfigMap(serviceConfigMap, druidService.getProperties(), "runtime.properties")
      );
      applyManifestYaml(druidService, createManifestYaml(druidService));
    }
  }

  protected void waitUntilServicesAreHealthy()
  {
    services.forEach(this::waitUntilServiceIsHealthy);
  }

  protected List<K3sDruidService> getServices()
  {
    return services;
  }

  private void createConfigMapForCommonProperties(EmbeddedDruidCluster cluster)
  {
    final Properties commonProperties = new Properties();
    commonProperties.putAll(cluster.getCommonProperties());
    commonProperties.remove("druid.extensions.modulesForEmbeddedTests");
    applyConfigMap(
        newConfigMap(COMMON_CONFIG_MAP, commonProperties, "common.runtime.properties")
    );
  }

  private void initKubernetesClient()
  {
    client = new KubernetesClientBuilder()
        .withConfig(Config.fromKubeconfig(getContainer().getKubeConfigYaml()))
        .build();
    closer.register(client);
  }

  protected void waitUntilPodsAreReady(String namespace)
  {
    client.pods().inNamespace(namespace).resources().forEach(this::waitUntilPodIsReady);
  }

  /**
   * Waits until pods have started running. Application readiness is checked separately by
   * {@link #waitUntilServicesAreHealthy()}.
   */
  protected void waitUntilPodsAreRunning(String namespace)
  {
    client.pods().inNamespace(namespace).resources().forEach(this::waitUntilPodIsRunning);
  }

  /**
   * Exposes the fabric8 {@link KubernetesClient} for tests that need to interact
   * with the cluster directly (e.g. discover task-launched peon pods).
   */
  public KubernetesClient getKubernetesClient()
  {
    return client;
  }

  @Override
  public void stop()
  {
    dumpKubernetesPodLogs();
    try {
      closer.close();
    }
    catch (Exception e) {
      log.error(e, "Could not close resources");
    }
    super.stop();
  }

  /**
   * Loads the given Docker image from the host Docker to the container. If the
   * image does not exist in the host Docker, the image will be pulled by the
   * K3s container itself.
   */
  private void loadLocalDockerImageIntoContainer(String localImageName, TestFolder testFolder)
  {
    ensureRunning();

    final File tempDir = testFolder.getOrCreateFolder("druid-k3s-image");
    final File tarFile = new File(tempDir, "druid-image.tar");

    final DefaultDockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder().build();

    try (
        final ApacheDockerHttpClient httpClient = new ApacheDockerHttpClient
            .Builder()
            .dockerHost(config.getDockerHost())
            .build();
        final DockerClient dockerClient = DockerClientImpl.getInstance(config, httpClient);
        final FileOutputStream tarOutputStream = new FileOutputStream(tarFile);
        final InputStream imageInputStream = dockerClient.saveImageCmd(localImageName).exec();
    ) {
      if (doesImageExistInHostDocker(localImageName, dockerClient)) {
        log.info("Transfering image[%s] from host Docker to K3s container.", localImageName);
      } else {
        log.info("Image[%s] will be pulled by K3s container as it does not exist host Docker.", localImageName);
        return;
      }

      imageInputStream.transferTo(tarOutputStream);
      log.info("Saved Docker image[%s] to tar[%s].", localImageName, tarFile);

      final String imagePathInContainer = "/tmp/druid-image.tar";
      getContainer().copyFileToContainer(
          MountableFile.forHostPath(tarFile.getAbsolutePath()),
          imagePathInContainer
      );

      getContainer().execInContainer("ctr", "-n", "k8s.io", "images", "import", imagePathInContainer);
      log.info("Image[%s] loaded into K3s containerd", localImageName);

      getContainer().execInContainer("rm", imagePathInContainer);
      FileUtils.deleteDirectory(tempDir);
    }
    catch (Exception e) {
      throw new ISE(e, "Failed to load local Docker image[%s]", localImageName);
    }
  }

  private boolean doesImageExistInHostDocker(String localImageName, DockerClient dockerClient)
  {
    try {
      dockerClient.inspectImageCmd(localImageName).exec();
      return true;
    }
    catch (NotFoundException e) {
      return false;
    }
  }

  private void applyConfigMap(ConfigMap configMap)
  {
    client.configMaps()
          .inNamespace(DRUID_NAMESPACE)
          .resource(configMap)
          .serverSideApply();
  }

  /**
   * Creates the manifest YAML for the given Druid service.
   */
  protected String createManifestYaml(K3sDruidService service)
  {
    return service.createManifestYaml(druidManifestTemplate, druidImageName);
  }

  /**
   * Applies the given service manifest YAML to the K3s cluster.
   */
  protected void applyManifestYaml(K3sDruidService service, String manifestYaml)
  {
    log.info("Applying manifest for service[%s]: %s", service.getName(), manifestYaml);
    try (ByteArrayInputStream bis = new ByteArrayInputStream(manifestYaml.getBytes(StandardCharsets.UTF_8))) {
      client.load(bis).inNamespace(DRUID_NAMESPACE).serverSideApply();
      log.info("Applied manifest for service[%s]", service.getName());
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Applies a YAML manifest file to the K3s container.
   */
  private void applyManifest(File manifest)
  {
    try (FileInputStream fis = new FileInputStream(manifest)) {
      client.load(fis).inNamespace(DRUID_NAMESPACE).serverSideApply();
      log.info("Applied manifest file[%s]", manifest);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Waits for the given pod to be ready.
   */
  private void waitUntilPodIsReady(PodResource pod)
  {
    try {
      pod.waitUntilCondition(
          p -> isPodReady(p) || isPodInFailureState(p),
          POD_READY_TIMEOUT_SECONDS,
          TimeUnit.SECONDS
      );
      final Pod currentPod = pod.get();
      if (isPodInFailureState(currentPod)) {
        throw createPodDiagnosticException(pod, null, "Pod entered a failure state");
      }
    }
    catch (KubernetesClientTimeoutException e) {
      throw createPodDiagnosticException(pod, e, "Timed out waiting for pod to be ready");
    }
  }

  private void waitUntilPodIsRunning(PodResource pod)
  {
    try {
      pod.waitUntilCondition(
          p -> isPodRunning(p) || isPodInFailureState(p),
          POD_READY_TIMEOUT_SECONDS,
          TimeUnit.SECONDS
      );
      final Pod currentPod = pod.get();
      if (isPodInFailureState(currentPod)) {
        throw createPodDiagnosticException(pod, null, "Pod entered a failure state");
      }
    }
    catch (KubernetesClientTimeoutException e) {
      throw createPodDiagnosticException(pod, e, "Timed out waiting for pod to start");
    }
  }

  private boolean isPodReady(Pod pod)
  {
    return pod != null &&
           pod.getStatus() != null &&
           pod.getStatus().getConditions() != null &&
           pod.getStatus().getConditions().stream().anyMatch(
               c -> "Ready".equals(c.getType()) && "True".equals(c.getStatus())
           );
  }

  private boolean isPodRunning(Pod pod)
  {
    return pod != null &&
           pod.getStatus() != null &&
           "Running".equals(pod.getStatus().getPhase());
  }

  private boolean isPodInFailureState(Pod pod)
  {
    final PodStatus status = pod == null ? null : pod.getStatus();
    return status != null &&
           ("Failed".equals(status.getPhase()) ||
            hasContainerFailure(status.getInitContainerStatuses()) ||
            hasContainerFailure(status.getContainerStatuses()));
  }

  private boolean hasContainerFailure(List<ContainerStatus> containerStatuses)
  {
    return containerStatuses != null && containerStatuses.stream().anyMatch(this::hasContainerFailure);
  }

  private boolean hasContainerFailure(ContainerStatus containerStatus)
  {
    if (containerStatus == null || containerStatus.getState() == null) {
      return false;
    }

    final ContainerState state = containerStatus.getState();
    if (state.getWaiting() != null && POD_FAILURE_REASONS.contains(state.getWaiting().getReason())) {
      return true;
    }

    return state.getTerminated() != null &&
           (!Objects.equals(0, state.getTerminated().getExitCode()) ||
            !"Completed".equals(state.getTerminated().getReason()));
  }

  private ISE createPodDiagnosticException(PodResource pod, Throwable cause, String message)
  {
    final Pod currentPod = getCurrentPod(pod);
    final String podName = currentPod == null || currentPod.getMetadata() == null
                           ? "unknown"
                           : currentPod.getMetadata().getName();
    final String podStatus = describePodStatus(currentPod);
    final String podEvents = getPodEvents(currentPod);
    final String podLogTail = getPodLogTail(pod);
    final String format = "%s for pod[%s]. Pod status[%s], pod events[%s], pod log tail[%s]";

    if (cause == null) {
      return new ISE(format, message, podName, podStatus, podEvents, podLogTail);
    }
    return new ISE(cause, format, message, podName, podStatus, podEvents, podLogTail);
  }

  private Pod getCurrentPod(PodResource pod)
  {
    try {
      return pod.get();
    }
    catch (Exception e) {
      return null;
    }
  }

  private String describePodStatus(Pod pod)
  {
    final PodStatus status = pod == null ? null : pod.getStatus();
    if (status == null) {
      return "unavailable";
    }

    return StringUtils.format(
        "phase[%s], conditions[%s], containerStatuses[%s], initContainerStatuses[%s]",
        status.getPhase(),
        status.getConditions(),
        describeContainerStatuses(status.getContainerStatuses()),
        describeContainerStatuses(status.getInitContainerStatuses())
    );
  }

  private String describeContainerStatuses(List<ContainerStatus> containerStatuses)
  {
    if (containerStatuses == null) {
      return "unavailable";
    }

    return containerStatuses.stream()
                           .map(
                               status -> StringUtils.format(
                                   "name[%s], ready[%s], restartCount[%s], state[%s], lastStateTerminated[%s]",
                                   status.getName(),
                                   status.getReady(),
                                   status.getRestartCount(),
                                   status.getState(),
                                   status.getLastState() == null ? null : status.getLastState().getTerminated()
                               )
                           )
                           .collect(Collectors.joining(", "));
  }

  private String getPodEvents(Pod pod)
  {
    if (client == null || pod == null || pod.getMetadata() == null) {
      return "unavailable";
    }

    final String namespace = pod.getMetadata().getNamespace();
    final String podName = pod.getMetadata().getName();
    if (namespace == null || podName == null) {
      return "unavailable";
    }

    try {
      final List<Event> events = client.v1()
                                       .events()
                                       .inNamespace(namespace)
                                       .withField("involvedObject.name", podName)
                                       .list()
                                       .getItems();
      return events.stream()
                   .map(
                       event -> StringUtils.format(
                           "type[%s], reason[%s], message[%s], count[%s], lastTimestamp[%s]",
                           event.getType(),
                           event.getReason(),
                           event.getMessage(),
                           event.getCount(),
                           event.getLastTimestamp()
                       )
                   )
                   .collect(Collectors.joining("; "));
    }
    catch (Exception e) {
      return StringUtils.format("unavailable: %s", e.getMessage());
    }
  }

  private String getPodLogTail(PodResource pod)
  {
    try {
      return pod.tailingLines(POD_LOG_TAIL_LINES).getLog();
    }
    catch (Exception e) {
      return StringUtils.format("unavailable: %s", e.getMessage());
    }
  }

  private void dumpKubernetesPodLogs()
  {
    if (client == null || containerLogsDirectory == null || !isRunning()) {
      return;
    }

    try {
      FileUtils.mkdirp(containerLogsDirectory);
      final List<Pod> pods = client.pods().inAnyNamespace().list().getItems();
      if (pods != null) {
        pods.forEach(this::dumpKubernetesPodLog);
      }
    }
    catch (Exception e) {
      log.warn(e, "Could not list Kubernetes pods for log collection");
    }
  }

  private void dumpKubernetesPodLog(Pod pod)
  {
    final ObjectMeta metadata = pod.getMetadata();
    if (metadata == null || metadata.getNamespace() == null || metadata.getName() == null) {
      return;
    }

    final String namespace = metadata.getNamespace();
    final String podName = metadata.getName();
    final File logFile = new File(
        containerLogsDirectory,
        StringUtils.format("%s-%s.log", namespace, podName)
    );

    try {
      final String podLog = client.pods()
                                 .inNamespace(namespace)
                                 .withName(podName)
                                 .getLog();
      Files.writeString(logFile.toPath(), podLog, StandardCharsets.UTF_8);
      log.info("Wrote Kubernetes pod log[%s]", logFile);
    }
    catch (Exception e) {
      log.warn(e, "Could not dump log for Kubernetes pod[%s] in namespace[%s]", podName, namespace);
    }
  }

  /**
   * Polls the health check endpoint of the given service until it is healthy.
   */
  private void waitUntilServiceIsHealthy(K3sDruidService service)
  {
    final URL url;
    try {
      url = new URL(service.getHealthCheckUrl());
    }
    catch (Exception e) {
      throw new ISE(e, "Could not construct URL for service[%s]", service.getName());
    }

    ITRetryUtil.retryUntilEquals(
        () -> {
          byte[] resp = url
              .openConnection()
              .getInputStream()
              .readAllBytes();
          String body = new String(resp, StandardCharsets.UTF_8);
          return body.contains("true");
        },
        true,
        1_000L,
        100,
        StringUtils.format("Service[%s] is healthy", service.getName())
    );
  }

  /**
   * Creates a new {@link ConfigMap} that can be applied to the K3s cluster.
   */
  private static ConfigMap newConfigMap(String name, Properties properties, String fileName)
  {
    try {
      // Serialize the properties
      StringWriter writer = new StringWriter();
      properties.store(writer, null);

      final ConfigMap configMap = new ConfigMap();
      ObjectMeta meta = new ObjectMeta();
      meta.setName(name);
      meta.setNamespace(DRUID_NAMESPACE);
      configMap.setMetadata(meta);
      configMap.setData(Map.of(fileName, writer.toString()));

      final String configMapYaml = Serialization.asYaml(configMap);
      log.info("Created config map[%s]: %s", name, configMapYaml);

      return configMap;
    }
    catch (Exception e) {
      throw new ISE(e, "Could not write config map[%s]", name);
    }
  }
}
