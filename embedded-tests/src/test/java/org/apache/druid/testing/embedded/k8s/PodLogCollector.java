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

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

/**
 * Collects logs from Kubernetes pods into a local directory.
 */
public final class PodLogCollector
{
  private static final Logger log = new Logger(PodLogCollector.class);

  private final KubernetesClient client;

  public PodLogCollector(KubernetesClient client)
  {
    this.client = client;
  }

  public void collectTo(File logDir)
  {
    if (client == null || logDir == null) {
      return;
    }

    try {
      FileUtils.mkdirp(logDir);
      final List<Pod> pods = client.pods().inAnyNamespace().list().getItems();
      if (pods != null) {
        pods.forEach(pod -> collectTo(pod, logDir));
      }
    }
    catch (Exception e) {
      log.warn(e, "Could not list Kubernetes pods for log collection");
    }
  }

  private void collectTo(Pod pod, File logDir)
  {
    final ObjectMeta metadata = pod.getMetadata();
    if (metadata == null || metadata.getNamespace() == null || metadata.getName() == null) {
      return;
    }

    final String namespace = metadata.getNamespace();
    final String podName = metadata.getName();
    final File logFile = new File(
        logDir,
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
}
