package com.hmdp.experience;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.mapper.BehaviorMapper;
import com.hmdp.mapper.MessageMapper;
import com.hmdp.service.IBehaviorJournalService;
import com.hmdp.service.IShopService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.impl.ExperienceServiceImpl;
import com.hmdp.utils.UserHolder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

class ExperienceServiceTest {
  private final ExperienceServiceImpl service = new ExperienceServiceImpl();
  private final BehaviorMapper behavior = mock(BehaviorMapper.class);
  private final MessageMapper messages = mock(MessageMapper.class);
  private final IShopService shops = mock(IShopService.class);
  private final IBehaviorJournalService journal = mock(IBehaviorJournalService.class);
  private final IVoucherOrderService orders = mock(IVoucherOrderService.class);
  private static final String EVENT = "ac62534f-7843-4d7d-a377-82cf27430742";

  @BeforeEach
  void prepare() {
    ReflectionTestUtils.setField(service, "behaviorMapper", behavior);
    ReflectionTestUtils.setField(service, "messageMapper", messages);
    ReflectionTestUtils.setField(service, "shopService", shops);
    ReflectionTestUtils.setField(service, "behaviorJournalService", journal);
    ReflectionTestUtils.setField(service, "voucherOrderService", orders);
    ReflectionTestUtils.setField(service, "metrics", new SimpleMeterRegistry());
  }

  @AfterEach
  void clearUser() {
    UserHolder.removeUser();
  }

  @Test
  void disabledExperienceDoesNotRevealRuntimeDetailsOrTouchData() {
    assertEquals(
        Collections.singletonMap("enabled", false),
        service.queryExperienceCapabilities().getData());
    assertFalse(service.traceShopCacheReads(1L).getSuccess());
    assertFalse(service.submitDemoVisitEvents(1L).getSuccess());
    assertFalse(service.queryVisitEventReceipts(Collections.singletonList(EVENT)).getSuccess());
    assertFalse(service.queryMyOrderProgress(1L).getSuccess());
    verifyNoInteractions(behavior, messages, shops, journal, orders);
  }

  @Test
  void validatesWholeReceiptBatchBeforeAnyDatabaseAccess() {
    enable();
    assertFalse(service.traceShopCacheReads(-1L).getSuccess());
    assertFalse(service.submitDemoVisitEvents(0L).getSuccess());
    assertFalse(service.queryVisitEventReceipts(Collections.emptyList()).getSuccess());
    assertFalse(service.queryVisitEventReceipts(Arrays.asList(EVENT, "invalid")).getSuccess());
    assertFalse(
        service.queryVisitEventReceipts(Arrays.asList(EVENT, EVENT, EVENT, EVENT)).getSuccess());
    verifyNoInteractions(behavior, shops, journal);
  }

  @Test
  @SuppressWarnings("unchecked")
  void persistedReceiptExposesNoIdentityOrUnrequestedEvents() {
    enable();
    Map<String, Object> stored = new HashMap<>();
    stored.put("shop_id", 1L);
    stored.put("user_id", 99L);
    stored.put("action", "VISIT");
    when(behavior.selectEvent(EVENT)).thenReturn(stored);
    List<Map<String, Object>> rows =
        (List<Map<String, Object>>)
            service.queryVisitEventReceipts(Collections.singletonList(EVENT)).getData();
    assertEquals(1, rows.size());
    assertEquals(EVENT, rows.get(0).get("eventId"));
    assertEquals("PERSISTED", rows.get(0).get("state"));
    assertEquals(3, rows.get(0).size());
    assertFalse(rows.get(0).containsKey("user_id"));
  }

  @Test
  void failedOwnershipCheckNeverQueriesEvents() {
    enable();
    assertFalse(service.queryMyOrderProgress(10L).getSuccess());
    UserDTO user = new UserDTO();
    user.setId(7L);
    UserHolder.saveUser(user);
    when(orders.queryMyOrder(10L)).thenReturn(Result.fail("订单不存在"));
    assertFalse(service.queryMyOrderProgress(10L).getSuccess());
    verifyNoInteractions(messages);
  }

  @Test
  void successfulOwnershipCheckStillBindsOwnerInMapperQuery() {
    enable();
    UserDTO user = new UserDTO();
    user.setId(7L);
    UserHolder.saveUser(user);
    when(orders.queryMyOrder(10L)).thenReturn(Result.ok());
    when(messages.selectOrderProgress(10L, 7L)).thenReturn(Collections.emptyList());
    assertTrue(service.queryMyOrderProgress(10L).getSuccess());
    verify(messages).selectOrderProgress(10L, 7L);
  }

  private void enable() {
    ReflectionTestUtils.setField(service, "enabled", true);
    ReflectionTestUtils.setField(service, "analyticsEnabled", true);
  }
}
