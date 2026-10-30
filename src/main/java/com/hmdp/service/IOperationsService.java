package com.hmdp.service;

import com.hmdp.dto.Result;

public interface IOperationsService {
  Result queryReservationsForReview();

  Result querySeckillDeadLetters();

  Result queryUnfinishedBusinessEvents();

  Result retryBusinessEvent(long eventId);

  Result queryMessageFailures();

  Result queryUnsentAnalyticsEvents();

  Result queryAnalyticsFailures();

  Result retryAnalyticsEvent(String eventId);

  Result queryInventoryReconciliationReport(long voucherId);

  Result publishActivity(long voucherId);

  Result recoverActivity(long voucherId);

  Result retryReservation(long orderId);

  Result markReservationFailedAndCompensateRedis(long orderId);
}
