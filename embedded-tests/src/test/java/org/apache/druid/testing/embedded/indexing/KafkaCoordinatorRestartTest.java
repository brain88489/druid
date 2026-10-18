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

package org.apache.druid.testing.embedded.indexing;

import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.StreamIngestResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KafkaCoordinatorRestartTest extends StreamIndexTestBase
{
  private final KafkaResource kafkaResource = new KafkaResource();

  @Override
  protected StreamIngestResource<?> getStreamIngestResource()
  {
    return kafkaResource;
  }

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return super.createCluster().useDefaultTimeoutForLatchableEmitter(240);
  }

  @Test
  public void test_supervisorRecovers_afterCoordinatorRestart() throws Exception
  {
    final String topic = "topic_" + dataSource;
    kafkaResource.createTopicWithPartitions(topic, 2);
    final SupervisorSpec supervisorSpec = createKafkaSupervisor(kafkaResource)
        .withId("supe_" + dataSource)
        .build(dataSource, topic);
    cluster.callApi().postSupervisor(supervisorSpec);

    final int initialRecords = publish1kRecords(topic, true);
    waitUntilPublishedRecordsAreIngested(initialRecords);

    coordinator.stop();
    final int recordsWhileStopped = publish1kRecords(topic, true);
    coordinator.start();
    final int recordsAfterRestart = publish1kRecords(topic, true);
    final int expectedRowCount = initialRecords + recordsWhileStopped + recordsAfterRestart;

    // Publication can commit while cancellation prevents the success callback from emitting ingest/rows/published.
    // Verify committed rows after the restart without depending on that metric being delivered.
    final long publishedRows = cluster.callApi().waitForResult(
        () -> IndexTaskUtils.getTotalRowCount(cluster.callApi().getVisibleUsedSegments(dataSource, overlord)),
        rows -> rows >= expectedRowCount
    ).withTimeoutMillis(240_000).go();
    Assertions.assertEquals(expectedRowCount, publishedRows);
    verifySupervisorIsRunningHealthy(supervisorSpec.getId());
    cluster.callApi().postSupervisor(supervisorSpec.createSuspendedSpec());
    kafkaResource.deleteTopic(topic);

    // Broker visibility can lag committed metadata while segments are being handed off.
    cluster.callApi().waitForAllSegmentsToBeQueryable(dataSource, coordinator, 240_000);
    final String expected = String.valueOf(expectedRowCount);
    final String actual = cluster.callApi().waitForResult(
        () -> cluster.runSql("SELECT COUNT(*) FROM %s", dataSource),
        expected::equals
    ).withTimeoutMillis(240_000).go();
    Assertions.assertEquals(expected, actual);
  }
}
