package com.hmdp.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hmdp.entity.VoucherOrder;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.annotations.Param;

/**
 * Mapper 接口
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface VoucherOrderMapper extends BaseMapper<VoucherOrder> {

  Long countByOrderId(@Param("orderId") Long orderId);

  Long countByVoucherId(@Param("voucherId") Long voucherId);

  List<Long> selectExpiredOrdinaryOrders();

  int initializeDeadline(
      @Param("timeoutMinutes") Integer timeoutMinutes, @Param("orderId") Long orderId);

  Map<String, Object> selectDeadline(@Param("orderId") Long orderId);

  List<Map<String, Object>> selectExpiryStateForUpdate(@Param("orderId") Long orderId);

  int closeExpiredOrder(@Param("orderId") Long orderId);

  int insertOrdinarySlot(@Param("userId") Long userId, @Param("voucherId") Long voucherId);

  List<Map<String, Object>> lockOrdinarySlot(
      @Param("userId") Long userId, @Param("voucherId") Long voucherId);

  int insertEventHistory(
      @Param("eventId") Long eventId,
      @Param("orderId") Long orderId,
      @Param("eventType") String eventType,
      @Param("occurredAt") Timestamp occurredAt);

  List<Map<String, Object>> selectBehaviorSnapshot(@Param("orderId") Long orderId);
}
