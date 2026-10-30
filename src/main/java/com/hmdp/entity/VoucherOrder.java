package com.hmdp.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import java.io.Serializable;
import java.time.LocalDateTime;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

/**
 * @author 虎哥
 * @since 2021-12-22
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
@TableName("tb_voucher_order")
public class VoucherOrder implements Serializable {

  private static final long serialVersionUID = 1L;

  /** 主键 */
  @TableId(value = "id", type = IdType.INPUT)
  private Long id;

  /** 下单的用户id */
  private Long userId;

  /** 购买的代金券id */
  private Long voucherId;

  /** 创建时应付金额，单位分；历史订单为空时兼容读取当前券价。 */
  private Long payValueSnapshot;
  /** 创建时券标题；后续改名不应改写该历史快照。 */
  private String titleSnapshot;
  /** 创建时使用规则。 */
  private String rulesSnapshot;
  /** 创建时券类型，0 普通券、1 秒杀券。 */
  private Integer voucherTypeSnapshot;
  /** 普通待支付订单的付款截止时间；免费券或无需关单的订单可为空。 */
  private LocalDateTime expireAt;
  /** 创建时所属店铺，用于订单行为分析归属。 */
  private Long shopIdSnapshot;

  /** 支付方式 1：余额支付；2：支付宝；3：微信 */
  private Integer payType;

  /** 订单状态，1：未支付；2：已支付；3：已核销；4：已取消；5：退款中；6：已退款 */
  private Integer status;

  /** 下单时间 */
  private LocalDateTime createTime;

  /** 支付时间 */
  private LocalDateTime payTime;

  /** 核销时间 */
  private LocalDateTime useTime;

  /** 退款时间 */
  private LocalDateTime refundTime;

  /** 更新时间 */
  private LocalDateTime updateTime;
}
