package com.hmdp.messaging;

import com.hmdp.entity.BusinessOutboxTask;
import lombok.Data;

/** The durable outbox ID is stable across sends; broker message IDs are transport metadata. */
@Data
public class BusinessEvent {
  private int schemaVersion = 1;
  private long eventId;
  private String kind;
  private long aggregateId;
  private long occurredAt;
  private Long deliverAt;

  public static BusinessEvent from(BusinessOutboxTask task) {
    BusinessEvent event = new BusinessEvent();
    event.eventId = task.getId();
    event.kind = task.getKind();
    event.aggregateId = task.getAggregateId();
    event.occurredAt = task.getCreatedAt().getTime();
    if (task.getDeliverAt() != null) {
      event.deliverAt = task.getDeliverAt().getTime();
    }
    return event;
  }
}
