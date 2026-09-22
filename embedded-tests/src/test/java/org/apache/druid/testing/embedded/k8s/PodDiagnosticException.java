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

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.PodResource;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Describes a Kubernetes pod failure with the status, events, and recent pod logs.
 */
public final class PodDiagnosticException extends ISE
{
  private static final int POD_LOG_TAIL_LINES = 100;

  private PodDiagnosticException(String formatText, Object... arguments)
  {
    super(formatText, arguments);
  }

  private PodDiagnosticException(Throwable cause, String formatText, Object... arguments)
  {
    super(cause, formatText, arguments);
  }

  public static PodDiagnosticException create(
      KubernetesClient client,
      PodResource pod,
      Throwable cause,
      String message
  )
  {
    final Pod currentPod = getCurrentPod(pod);
    final String podName = currentPod == null || currentPod.getMetadata() == null
                           ? "unknown"
                           : currentPod.getMetadata().getName();
    final String podStatus = describePodStatus(currentPod);
    final String podEvents = getPodEvents(client, currentPod);
    final String podLogTail = getPodLogTail(pod);
    final String format = "%s for pod[%s]. Pod status[%s], pod events[%s], pod log tail[%s]";

    if (cause == null) {
      return new PodDiagnosticException(format, message, podName, podStatus, podEvents, podLogTail);
    }
    return new PodDiagnosticException(cause, format, message, podName, podStatus, podEvents, podLogTail);
  }

  private static Pod getCurrentPod(PodResource pod)
  {
    try {
      return pod.get();
    }
    catch (Exception e) {
      return null;
    }
  }

  private static String describePodStatus(Pod pod)
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

  private static String describeContainerStatuses(List<ContainerStatus> containerStatuses)
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

  private static String getPodEvents(KubernetesClient client, Pod pod)
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
      return events == null
             ? "unavailable"
             : events.stream()
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

  private static String getPodLogTail(PodResource pod)
  {
    try {
      return pod.tailingLines(POD_LOG_TAIL_LINES).getLog();
    }
    catch (Exception e) {
      return StringUtils.format("unavailable: %s", e.getMessage());
    }
  }
}
