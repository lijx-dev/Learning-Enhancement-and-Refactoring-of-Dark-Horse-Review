package com.hmdp.reliability;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.hmdp.mapper.SeckillReservationMapper;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.impl.SeckillServiceImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionTemplate;

class SeckillMessageTest {
  @Test
  @SuppressWarnings("unchecked")
  void registrationOutageDoesNotAcknowledgeOrQuarantineAnUnregisteredRequest() {
    SeckillReservationMapper reservations = mock(SeckillReservationMapper.class);
    doThrow(new DataAccessResourceFailureException("database unavailable"))
        .when(reservations)
        .insertReservation(anyLong(), anyLong(), anyLong());
    StringRedisTemplate redis = mock(StringRedisTemplate.class);
    ObjectProvider<IVoucherOrderService> orders = mock(ObjectProvider.class);
    SeckillServiceImpl worker =
        new SeckillServiceImpl(
            redis,
            mock(TransactionTemplate.class),
            mock(RedissonClient.class),
            orders,
            new SimpleMeterRegistry());
    ReflectionTestUtils.setField(worker, "seckillReservationMapper", reservations);
    Map<Object, Object> payload = new HashMap<>();
    payload.put("id", "123");
    payload.put("voucherId", "456");
    payload.put("userId", "789");
    ReflectionTestUtils.invokeMethod(
        worker,
        "handleOrderStreamMessage",
        MapRecord.create(SeckillServiceImpl.STREAM, payload).withId(RecordId.of("1-0")));
    verify(reservations).insertReservation(123L, 456L, 789L);
    verifyNoMoreInteractions(reservations);
    verifyNoInteractions(redis, orders);
    worker.stop();
  }
}
