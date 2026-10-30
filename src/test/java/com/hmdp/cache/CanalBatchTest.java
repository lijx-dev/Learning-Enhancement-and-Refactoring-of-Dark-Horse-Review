package com.hmdp.cache;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.hmdp.service.ICacheCheckpointService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.redisson.api.RedissonClient;
import org.springframework.test.util.ReflectionTestUtils;

class CanalBatchTest {
  @Test
  void checkpointAndAckFollowSuccessfulProjectionButFailureRollsBack() throws Exception {
    CacheChangeProjector projector = mock(CacheChangeProjector.class);
    ICacheCheckpointService checkpoint = mock(ICacheCheckpointService.class);
    CanalConnector connector = mock(CanalConnector.class);
    CanalCacheConsumer consumer =
        new CanalCacheConsumer(projector, mock(RedissonClient.class), new SimpleMeterRegistry());
    ReflectionTestUtils.setField(consumer, "cacheCheckpointService", checkpoint);
    ReflectionTestUtils.setField(consumer, "destination", "example");
    CanalEntry.Entry entry =
        CanalEntry.Entry.newBuilder()
            .setHeader(
                CanalEntry.Header.newBuilder().setLogfileName("mysql-bin.1").setLogfileOffset(100))
            .build();
    when(projector.isRelevantEntry(entry)).thenReturn(true);
    Message message = new Message(10, Collections.singletonList(entry));
    consumer.processBatch(connector, message);
    InOrder order = inOrder(projector, checkpoint, connector);
    order.verify(projector).projectEntryChanges(entry);
    order
        .verify(checkpoint)
        .saveCheckpoint(anyString(), anyLong(), anyString(), anyLong(), anyLong());
    order.verify(connector).ack(10);
    reset(projector, checkpoint, connector);
    doThrow(new IllegalStateException("Redis unavailable"))
        .when(projector)
        .projectEntryChanges(entry);
    assertThrows(IllegalStateException.class, () -> consumer.processBatch(connector, message));
    verify(connector).rollback(10);
    verify(connector, never()).ack(anyLong());
    verifyNoInteractions(checkpoint);
    consumer.stop();
  }

  @Test
  void checkpointFailureAlsoLeavesBatchUnacknowledged() {
    CacheChangeProjector projector = mock(CacheChangeProjector.class);
    ICacheCheckpointService checkpoint = mock(ICacheCheckpointService.class);
    CanalConnector connector = mock(CanalConnector.class);
    CanalCacheConsumer consumer =
        new CanalCacheConsumer(projector, mock(RedissonClient.class), new SimpleMeterRegistry());
    ReflectionTestUtils.setField(consumer, "cacheCheckpointService", checkpoint);
    ReflectionTestUtils.setField(consumer, "destination", "example");
    CanalEntry.Entry entry =
        CanalEntry.Entry.newBuilder().setHeader(CanalEntry.Header.newBuilder()).build();
    when(projector.isRelevantEntry(entry)).thenReturn(true);
    doThrow(new IllegalStateException("MySQL unavailable"))
        .when(checkpoint)
        .saveCheckpoint(anyString(), anyLong(), anyString(), anyLong(), anyLong());
    assertThrows(
        IllegalStateException.class,
        () -> consumer.processBatch(connector, new Message(11, Collections.singletonList(entry))));
    verify(connector).rollback(11);
    verify(connector, never()).ack(anyLong());
    consumer.stop();
  }

  @Test
  void excludedTransactionBatchDoesNotCreateCheckpointFeedbackLoop() {
    CacheChangeProjector projector = mock(CacheChangeProjector.class);
    ICacheCheckpointService checkpoint = mock(ICacheCheckpointService.class);
    CanalConnector connector = mock(CanalConnector.class);
    CanalCacheConsumer consumer =
        new CanalCacheConsumer(projector, mock(RedissonClient.class), new SimpleMeterRegistry());
    ReflectionTestUtils.setField(consumer, "cacheCheckpointService", checkpoint);
    CanalEntry.Entry boundary =
        CanalEntry.Entry.newBuilder().setEntryType(CanalEntry.EntryType.TRANSACTIONEND).build();
    consumer.processBatch(connector, new Message(12, Collections.singletonList(boundary)));
    verify(connector).ack(12);
    verifyNoInteractions(checkpoint);
    consumer.stop();
  }
}
