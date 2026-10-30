package com.hmdp.mapper;

import java.util.List;
import java.util.Map;
import org.apache.ibatis.annotations.Param;

/** 数据访问接口，SQL 统一维护在对应的 Mapper XML 中。 */
public interface BehaviorMapper {

  List<Map<String, Object>> selectUnsentEvents();

  List<Map<String, Object>> selectFailures();

  int retryReviewEvent(@Param("eventId") String eventId);

  int insertFailure(
      @Param("topic") String topic,
      @Param("partitionId") Integer partitionId,
      @Param("messageOffset") Long messageOffset,
      @Param("payload") String payload,
      @Param("reason") String reason);

  int insertEvent(
      @Param("eventId") String eventId,
      @Param("shopId") Long shopId,
      @Param("userId") Long userId,
      @Param("action") String action,
      @Param("occurredAtMs") Long occurredAtMs,
      @Param("bucketMs") Long bucketMs);

  Map<String, Object> selectEvent(@Param("eventId") String eventId);

  int accumulateBucket(
      @Param("shopId") Long shopId,
      @Param("bucketMs") Long bucketMs,
      @Param("visits") Integer visits,
      @Param("orders") Integer orders,
      @Param("payments") Integer payments);

  int insertVisitUser(
      @Param("shopId") Long shopId, @Param("userId") Long userId, @Param("bucketMs") Long bucketMs);

  List<Map<String, Object>> selectRanking(
      @Param("uvFromMs") Long uvFromMs,
      @Param("uvToMs") Long uvToMs,
      @Param("bucketFromMs") Long bucketFromMs,
      @Param("bucketToMs") Long bucketToMs,
      @Param("limit") Integer limit);

  int insertOutboxEvent(
      @Param("eventId") String eventId,
      @Param("shopId") Long shopId,
      @Param("payload") String payload);

  List<Map<String, Object>> selectClaimableEvents();

  int claimEvent(@Param("leaseOwner") String leaseOwner, @Param("eventId") String eventId);

  int renewLease(@Param("eventId") String eventId, @Param("leaseOwner") String leaseOwner);

  int markSent(@Param("eventId") String eventId, @Param("leaseOwner") String leaseOwner);

  int recordSendFailure(
      @Param("state") String state,
      @Param("attempts") Integer attempts,
      @Param("lastError") String lastError,
      @Param("delaySeconds") Long delaySeconds,
      @Param("eventId") String eventId,
      @Param("leaseOwner") String leaseOwner);

  int redriveMissingEvents();

  Long countPendingEvents();

  Long countReviewEvents();
}
