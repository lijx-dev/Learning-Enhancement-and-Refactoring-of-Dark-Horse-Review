package com.hmdp.mapper;

import com.hmdp.entity.SeckillReservation;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.annotations.Param;

/** 数据访问接口，SQL 统一维护在对应的 Mapper XML 中。 */
public interface SeckillReservationMapper {

  List<Map<String, Object>> selectDeadLetters();

  Long countByVoucherId(@Param("voucherId") Long voucherId);

  Long countReviewRequests();

  int recordAttempt(@Param("reason") String reason, @Param("orderId") Long orderId);

  SeckillReservation selectAttemptState(@Param("orderId") Long orderId);

  SeckillReservation selectStateForUpdate(@Param("orderId") Long orderId);

  int markProcessingForReview(@Param("reason") String reason, @Param("orderId") Long orderId);

  int insertDeadLetter(
      @Param("messageId") String messageId,
      @Param("orderId") Long orderId,
      @Param("payload") String payload,
      @Param("reason") String reason);

  SeckillReservation selectFulfillment(@Param("orderId") Long orderId);

  int insertReservation(
      @Param("orderId") Long orderId,
      @Param("voucherId") Long voucherId,
      @Param("userId") Long userId);

  SeckillReservation selectReservation(@Param("orderId") Long orderId);

  int markSuccess(@Param("orderId") Long orderId);

  String selectState(@Param("orderId") Long orderId);

  List<Map<String, Object>> selectOwnedRequest(
      @Param("orderId") Long orderId, @Param("userId") Long userId);

  Long countUnfinishedRequests(@Param("voucherId") Long voucherId);

  List<Map<String, Object>> selectSuccessfulBuyers(@Param("voucherId") Long voucherId);

  SeckillReservation selectReservationForUpdate(@Param("orderId") Long orderId);

  int resetForRetry(@Param("orderId") Long orderId);

  int markFailed(@Param("orderId") Long orderId);

  List<Map<String, Object>> selectReviewRequests();
}
