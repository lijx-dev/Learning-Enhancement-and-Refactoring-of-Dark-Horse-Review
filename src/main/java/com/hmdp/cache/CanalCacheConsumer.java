package com.hmdp.cache;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.hmdp.service.ICacheCheckpointService;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CanalCacheConsumer {

  @Resource
  private ICacheCheckpointService cacheCheckpointService;

  private final CacheChangeProjector projector;
  private final RedissonClient redisson;
  private final MeterRegistry metrics;

  @Value("${hmdp.cdc.enabled:false}")
  private boolean enabled;

  @Value("${hmdp.cdc.host:127.0.0.1}")
  private String host;

  @Value("${hmdp.cdc.port:11111}")
  private int port;

  @Value("${hmdp.cdc.destination:example}")
  private String destination;

  @Value("${hmdp.cdc.schema:hmdp}")
  private String schema;

  private final AtomicLong connected = new AtomicLong();
  private final AtomicLong lastPoll = new AtomicLong();

  private final ExecutorService worker =
      Executors.newSingleThreadExecutor(
          r -> {
            Thread t = new Thread(r, "cache-canal");
            t.setDaemon(true);
            return t;
          });
  private volatile boolean running;
  private volatile CanalConnector active;

  public CanalCacheConsumer(
      CacheChangeProjector projector, RedissonClient redisson, MeterRegistry metrics) {
    this.projector = projector;
    this.redisson = redisson;
    this.metrics = metrics;
    metrics.gauge("hmdp.cdc.connected", connected);
    metrics.gauge(
        "hmdp.cdc.poll.age.seconds",
        lastPoll,
        value ->
            value.get() == 0
                ? 0
                : Math.max(0, (System.currentTimeMillis() - value.get()) / 1000.0));
  }

  @PostConstruct
  public void start() {
    metrics.gauge("hmdp.cdc.enabled", enabled ? 1 : 0);
    if (enabled) {
      if (!schema.matches("[A-Za-z0-9_]+")) {
        throw new IllegalArgumentException("CDC schema 名称只允许字母、数字和下划线");
      }
      running = true;
      worker.submit(this::consume);
    }
  }

  private void consume() {
    while (running && !Thread.currentThread().isInterrupted()) {
      RLock leader = redisson.getLock("lock:cache-cdc:" + destination);
      try {
        if (!leader.tryLock()) {
          Thread.sleep(1000);
          continue;
        }
        CanalConnector connector =
            CanalConnectors.newSingleConnector(
                new InetSocketAddress(host, port), destination, "", "");
        active = connector;
        try {
          connector.connect();
          connector.subscribe(schema + "\\.(tb_shop|tb_voucher|tb_seckill_voucher)");
          connector.rollback();
          connected.set(1);
          while (running && !Thread.currentThread().isInterrupted()) {
            Message batch = connector.getWithoutAck(200, 1L, TimeUnit.SECONDS);
            lastPoll.set(System.currentTimeMillis());
            if (batch.getId() < 0 || batch.getEntries().isEmpty()) {
              Thread.sleep(100);
              continue;
            }
            processBatch(connector, batch);
          }
        } finally {
          connected.set(0);
          connector.disconnect();
          active = null;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (RuntimeException e) {
        connected.set(0);
        metrics.counter("hmdp.cdc.connections", "result", "error").increment();
        if (running) {
          log.warn("Canal 暂不可用，将重连: {}", e.getMessage());
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
        }
      } finally {
        try {
          if (leader.isHeldByCurrentThread()) {
            leader.unlock();
          }
        } catch (RuntimeException unlockFailure) {
          log.warn("CDC 主节点锁释放失败，将由租约回收");
        }
      }
    }
  }

  void processBatch(CanalConnector connector, Message batch) {
    try {
      boolean relevant = false;
      for (CanalEntry.Entry entry : batch.getEntries()) {
        if (projector.isRelevantEntry(entry)) {
          relevant = true;
        }
        projector.projectEntryChanges(entry);
      }
      // Checkpoint writes themselves produce transaction boundaries in binlog.
      // Ack batches containing only excluded tables without writing another checkpoint.
      if (!relevant) {
        connector.ack(batch.getId());
        metrics.counter("hmdp.cdc.batches", "result", "ignored").increment();
        return;
      }
      CanalEntry.Header tail = batch.getEntries().get(batch.getEntries().size() - 1).getHeader();
      cacheCheckpointService.saveCheckpoint(
          destination,
          batch.getId(),
          tail.getLogfileName(),
          tail.getLogfileOffset(),
          tail.getExecuteTime());
      connector.ack(batch.getId());
      metrics.counter("hmdp.cdc.batches", "result", "success").increment();
      if (tail.getExecuteTime() > 0) {
        metrics
            .timer("hmdp.cdc.event.delay")
            .record(
                Math.max(0, System.currentTimeMillis() - tail.getExecuteTime()),
                TimeUnit.MILLISECONDS);
      }
    } catch (Exception failure) {
      metrics.counter("hmdp.cdc.batches", "result", "retry").increment();
      connector.rollback(batch.getId());
      throw new IllegalStateException("缓存 CDC 批次失败，保留位点重放", failure);
    }
  }

  @PreDestroy
  public void stop() {
    running = false;
    worker.shutdownNow();
    CanalConnector connector = active;
    if (connector != null) {
      try {
        connector.disconnect();
      } catch (RuntimeException ignored) {
      }
    }
    try {
      worker.awaitTermination(2, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
