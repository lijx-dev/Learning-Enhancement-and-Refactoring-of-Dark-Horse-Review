package com.hmdp.mapper;

import java.util.List;
import java.util.Map;
import org.apache.ibatis.annotations.Param;

/** 数据访问接口，SQL 统一维护在对应的 Mapper XML 中。 */
public interface MessageMapper {

  List<Map<String, Object>> selectOrderProgress(
      @Param("orderId") Long orderId, @Param("userId") Long userId);

  List<Map<String, Object>> selectFailures();

  Long countInboxEvent(
      @Param("consumerGroup") String consumerGroup, @Param("eventId") Long eventId);

  int insertInboxEvent(
      @Param("consumerGroup") String consumerGroup, @Param("eventId") Long eventId);

  int insertFailure(
      @Param("consumerGroup") String consumerGroup,
      @Param("eventKey") String eventKey,
      @Param("eventId") Long eventId,
      @Param("brokerMessageId") String brokerMessageId,
      @Param("payload") String payload,
      @Param("reason") String reason);
}
