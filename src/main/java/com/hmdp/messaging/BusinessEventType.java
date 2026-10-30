package com.hmdp.messaging;

/** 消息类型与逻辑消费组；名称同时是数据库和消息中的稳定协议值。 */
public enum BusinessEventType {
  FEED("feed"),
  LIKE("relations"),
  FOLLOW("relations"),
  MEDIA_DELETE("media"),
  SHOP_INVALIDATE("cache"),
  SECKILL_PUBLISH("activities"),
  SECKILL_RETRY("activities"),
  SECKILL_FULFILL("fulfillment"),
  ORDER_EXPIRE("deadlines"),
  ORDER_CREATED("orders"),
  ORDER_PAID("orders"),
  ORDER_CLOSED("orders");

  private final String consumerGroup;

  BusinessEventType(String consumerGroup) {
    this.consumerGroup = consumerGroup;
  }

  public String getConsumerGroup() {
    return consumerGroup;
  }

  public static BusinessEventType from(String kind) {
    for (BusinessEventType type : values()) {
      if (type.name().equals(kind)) {
        return type;
      }
    }
    throw new IllegalArgumentException("未知事件类型: " + kind);
  }
}
