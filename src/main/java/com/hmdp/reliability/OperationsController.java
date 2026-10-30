package com.hmdp.reliability;

import com.hmdp.dto.Result;
import com.hmdp.service.IOperationsService;
import javax.annotation.Resource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/ops")
public class OperationsController {
  @Resource
  private IOperationsService operationsService;

  @GetMapping("/seckill/tasks")
  public Result queryReservationsForReview() {
    return operationsService.queryReservationsForReview();
  }

  @GetMapping("/seckill/dead-letters")
  public Result querySeckillDeadLetters() {
    return operationsService.querySeckillDeadLetters();
  }

  @GetMapping("/outbox")
  public Result queryUnfinishedBusinessEvents() {
    return operationsService.queryUnfinishedBusinessEvents();
  }

  @PostMapping("/outbox/{id}/retry")
  public Result retryBusinessEvent(@PathVariable("id") long eventId) {
    return operationsService.retryBusinessEvent(eventId);
  }

  @GetMapping("/messages/failures")
  public Result queryMessageFailures() {
    return operationsService.queryMessageFailures();
  }

  @GetMapping("/analytics/outbox")
  public Result queryUnsentAnalyticsEvents() {
    return operationsService.queryUnsentAnalyticsEvents();
  }

  @GetMapping("/analytics/failures")
  public Result queryAnalyticsFailures() {
    return operationsService.queryAnalyticsFailures();
  }

  @PostMapping("/analytics/outbox/{eventId}/retry")
  public Result retryAnalyticsEvent(@PathVariable String eventId) {
    return operationsService.retryAnalyticsEvent(eventId);
  }

  @GetMapping("/seckill/{id}/reconcile")
  public Result queryInventoryReconciliationReport(@PathVariable("id") long voucherId) {
    return operationsService.queryInventoryReconciliationReport(voucherId);
  }

  @PostMapping("/seckill/{id}/publish")
  public Result publishActivity(@PathVariable("id") long voucherId) {
    return operationsService.publishActivity(voucherId);
  }

  @PostMapping("/seckill/{id}/recover")
  public Result recoverActivity(@PathVariable("id") long voucherId) {
    return operationsService.recoverActivity(voucherId);
  }

  @PostMapping("/seckill/requests/{id}/retry")
  public Result retryReservation(@PathVariable("id") long orderId) {
    return operationsService.retryReservation(orderId);
  }

  @PostMapping("/seckill/requests/{id}/fail")
  public Result markReservationFailedAndCompensateRedis(@PathVariable("id") long orderId) {
    return operationsService.markReservationFailedAndCompensateRedis(orderId);
  }
}
