package com.hmdp.messaging;

public interface EventPublisher {
  void publish(BusinessEvent event);
}
