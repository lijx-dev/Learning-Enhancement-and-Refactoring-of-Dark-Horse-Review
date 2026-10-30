package com.hmdp.mapper;

import com.hmdp.entity.BusinessOutboxTask;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.annotations.Param;

/** 数据访问接口，SQL 统一维护在对应的 Mapper XML 中。 */
public interface BusinessOutboxMapper {

  int insertEvent(
      @Param("kind") String kind,
      @Param("aggregateId") Long aggregateId,
      @Param("userId") Long userId);

  int insertMediaDeletion(
      @Param("aggregateId") Long aggregateId,
      @Param("userId") Long userId,
      @Param("resourcePath") String resourcePath);

  int insertDeadline(
      @Param("orderId") Long orderId,
      @Param("userId") Long userId,
      @Param("deadline") Timestamp deadline);

  List<Map<String, Object>> selectUnfinishedEvents();

  BusinessOutboxTask selectReviewEvent(@Param("eventId") Long eventId);

  int retryReviewEvent(@Param("eventId") Long eventId);

  int insertSeckillFulfillment(
      @Param("orderId") Long orderId,
      @Param("userId") Long userId,
      @Param("idempotencyKey") String idempotencyKey);

  int retryFulfillment(@Param("idempotencyKey") String idempotencyKey);

  int insertSeckillRetry(@Param("orderId") Long orderId);

  BusinessOutboxTask selectEventForUpdate(@Param("eventId") Long eventId);

  BusinessOutboxTask selectEventById(@Param("eventId") Long eventId);

  int markDuplicateDone(@Param("eventId") Long eventId);

  Long selectCursor(@Param("eventId") Long eventId);

  int insertFeedContinuation(
      @Param("kind") String kind,
      @Param("aggregateId") Long aggregateId,
      @Param("userId") Long userId,
      @Param("cursorId") Long cursorId,
      @Param("createdAt") Timestamp createdAt);

  int markConsumed(@Param("eventId") Long eventId);

  int recordConsumeFailure(@Param("lastError") String lastError, @Param("eventId") Long eventId);

  int markReview(@Param("eventId") Long eventId);

  List<BusinessOutboxTask> selectClaimableEvents(@Param("limit") Integer limit);

  int claimEvent(
      @Param("leaseOwner") String leaseOwner,
      @Param("leaseSeconds") Integer leaseSeconds,
      @Param("eventId") Long eventId);

  int renewLease(
      @Param("leaseSeconds") Integer leaseSeconds,
      @Param("eventId") Long eventId,
      @Param("leaseOwner") String leaseOwner);

  int markSent(@Param("eventId") Long eventId, @Param("leaseOwner") String leaseOwner);

  int deferDeadline(@Param("eventId") Long eventId, @Param("leaseOwner") String leaseOwner);

  int recordSendFailure(
      @Param("attempts") Integer attempts,
      @Param("delaySeconds") Long delaySeconds,
      @Param("state") String state,
      @Param("lastError") String lastError,
      @Param("eventId") Long eventId,
      @Param("leaseOwner") String leaseOwner);

  int redriveUnconsumedEvents(
      @Param("sentAgeSeconds") Integer sentAgeSeconds,
      @Param("deadlineAgeSeconds") Integer deadlineAgeSeconds);

  Long countPendingEvents();

  Long countReviewEvents();

  Long countDueSentEvents();

  int updateCursor(@Param("cursorId") Long cursorId, @Param("eventId") Long eventId);
}
