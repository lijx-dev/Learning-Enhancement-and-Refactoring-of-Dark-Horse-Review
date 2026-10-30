package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.conditions.query.LambdaQueryChainWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.enums.OrderStatus;
import com.hmdp.enums.VoucherType;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.IOrderLifecycleService;
import com.hmdp.service.ISeckillService;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 服务实现类
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder>
    implements IVoucherOrderService {

  @Value("${citylens.payment.mock-enabled:false}")
  private boolean mockPaymentEnabled;

  @Resource
  private ISeckillVoucherService seckillVoucherService;

  @Resource
  private IVoucherService voucherService;

  @Resource
  private RedisIdWorker redisIdWorker;

  @Resource
  private RedissonClient redissonClient;

  @Resource
  private StringRedisTemplate stringRedisTemplate;

  @Resource
  private TransactionTemplate transactionTemplate;

  @Resource
  private IOrderLifecycleService lifecycle;

  private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

  static {
    SECKILL_SCRIPT = new DefaultRedisScript<>();
    SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
    SECKILL_SCRIPT.setResultType(Long.class);
  }

  @Resource
  private ISeckillService coordinator;

  @Override
  public void persistSeckill(VoucherOrder order) {
    createVoucherOrder(order);
  }

  protected void createVoucherOrder(VoucherOrder voucherOrder) {
    Long userId = voucherOrder.getUserId();
    Long voucherId = voucherOrder.getVoucherId();
    RLock lock = redissonClient.getLock("lock:order:" + userId);
    if (!lock.tryLock()) {
      throw new IllegalStateException("订单锁忙，保留消息重试");
    }
    try {
      transactionTemplate.execute(
          status -> {
            if (coordinator != null && !coordinator.lockReservationForFulfillment(voucherOrder)) {
              return null;
            }
            VoucherOrder existing = getById(voucherOrder.getId());
            if (existing != null) {
              if (!userId.equals(existing.getUserId())
                  || !voucherId.equals(existing.getVoucherId())) {
                throw new IllegalStateException("订单号冲突");
              }
              if (coordinator != null) {
                coordinator.markReservationSuccessful(voucherOrder.getId());
              }
              return null;
            }
            if (query().eq("user_id", userId).eq("voucher_id", voucherId).count() > 0) {
              throw new IllegalStateException("资格对应的订单号不一致，保留消息核对");
            }
            boolean deducted =
                seckillVoucherService
                    .update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherId)
                    .gt("stock", 0)
                    .update();
            if (!deducted) {
              throw new IllegalStateException("库存不足，保留消息核对");
            }
            Voucher voucher = voucherService.getById(voucherId);
            if (voucher == null) {
              throw new IllegalStateException("商品不存在，保留消息核对");
            }
            snapshot(voucherOrder, voucher);
            voucherOrder.setStatus(initialOrderStatus(voucher));
            if (!save(voucherOrder)) {
              throw new IllegalStateException("订单写入失败，回滚库存");
            }
            if (coordinator != null) {
              coordinator.markReservationSuccessful(voucherOrder.getId());
            }
            return null;
          });
    } finally {
      lock.unlock();
    }
  }

  @Autowired(required = false)
  private MeterRegistry metrics;

  @Override
  public Result seckillVoucher(Long voucherId) {
    if (voucherId == null || voucherId <= 0) {
      return Result.fail("秒杀券参数无效");
    }
    Long userId = UserHolder.getUser().getId();
    long orderId = redisIdWorker.nextId("order");
    // 1.执行lua脚本
    long started = System.nanoTime();
    Long result;
    try {
      result =
          stringRedisTemplate.execute(
              SECKILL_SCRIPT,
              Arrays.asList(
                  "seckill:stock:" + voucherId,
                  "seckill:order:" + voucherId,
                  "stream.orders",
                  "seckill:activity:" + voucherId,
                  "seckill:requests:" + voucherId,
                  "seckill:request:" + orderId),
              voucherId.toString(),
              userId.toString(),
              String.valueOf(orderId));
    } catch (RuntimeException unavailable) {
      if (metrics != null) {
        metrics.counter("hmdp.seckill.admissions", "result", "transport_error").increment();
      }
      return Result.fail("订单服务暂时不可用，请稍后查询订单状态");
    } finally {
      if (metrics != null) {
        metrics
            .timer("hmdp.seckill.admission")
            .record(System.nanoTime() - started, TimeUnit.NANOSECONDS);
      }
    }
    if (metrics != null) {
      metrics
          .counter("hmdp.seckill.admissions", "result", admissionMetricLabel(result))
          .increment();
    }
    if (result == null) {
      return Result.fail("订单服务暂时不可用");
    }
    int admissionCode = result.intValue();
    // 2.判断结果是否为0
    if (admissionCode != 0) {
      // 2.1.不为0 ，代表没有购买资格
      if (admissionCode == 2) {
        Object original =
            stringRedisTemplate
                .opsForHash()
                .get("seckill:requests:" + voucherId, userId.toString());
        if (original != null) {
          return Result.ok(Long.parseLong(original.toString()));
        }
        VoucherOrder existing =
            query()
                .eq("user_id", userId)
                .eq("voucher_id", voucherId)
                .orderByDesc("create_time")
                .last("LIMIT 1")
                .one();
        if (existing != null) {
          return Result.ok(existing.getId());
        }
        return Result.fail("已有抢购资格，请查询处理状态");
      }
      return Result.fail(
          admissionCode == 1 ? "库存不足" : admissionCode == 3 ? "活动尚未就绪、已暂停或不在购买时段" : "活动数据异常，请稍后重试");
    }
    // 3.返回订单id
    return Result.ok(orderId);
  }

  @Override
  public Result orderOrdinaryVoucher(Long voucherId) {
    Long userId = UserHolder.getUser().getId();
    RLock lock = redissonClient.getLock("lock:order:ordinary:" + userId);
    if (!lock.tryLock()) {
      return Result.fail("订单正在处理，请稍后查询");
    }
    try {
      return transactionTemplate.execute(
          tx -> {
            baseMapper.insertOrdinarySlot(userId, voucherId);
            baseMapper.lockOrdinarySlot(userId, voucherId);
            Voucher voucher = voucherService.getById(voucherId);
            if (voucher == null
                || voucher.getStatus() == null
                || voucher.getStatus() != 1
                || voucher.getType() == null
                || !VoucherType.ORDINARY.matches(voucher.getType())) {
              return Result.fail("普通券不存在或已下架");
            }
            VoucherOrder existing =
                query()
                    .eq("user_id", userId)
                    .eq("voucher_id", voucherId)
                    .ne("status", OrderStatus.CANCELLED.getCode())
                    .orderByDesc("create_time")
                    .last("LIMIT 1")
                    .one();
            if (existing != null) {
              return Result.ok(existing.getId());
            }
            VoucherOrder order = new VoucherOrder();
            order.setId(redisIdWorker.nextId("order"));
            order.setUserId(userId);
            order.setVoucherId(voucherId);
            snapshot(order, voucher);
            // 免费券直接领取；付费券只创建待支付订单，不能冒充支付成功。
            order.setStatus(initialOrderStatus(voucher));
            if (!save(order)) {
              return Result.fail("创建订单失败");
            }
            lifecycle.registerOrderCreatedEventAndDeadline(order.getId());
            return Result.ok(order.getId());
          });
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Result queryMyOrder(Long orderId) {
    VoucherOrder order = getById(orderId);
    if (order == null || !UserHolder.getUser().getId().equals(order.getUserId())) {
      return Result.fail("订单不存在");
    }
    Map<String, Object> detail = BeanUtil.beanToMap(order);
    Voucher voucher = voucherService.getById(order.getVoucherId());
    if (voucher != null) {
      detail.put(
          "voucherType",
          order.getVoucherTypeSnapshot() == null
              ? voucher.getType()
              : order.getVoucherTypeSnapshot());
      detail.put(
          "payValue",
          order.getPayValueSnapshot() == null
              ? voucher.getPayValue()
              : order.getPayValueSnapshot());
      detail.put("shopId", voucher.getShopId());
      detail.put(
          "voucherTitle",
          order.getTitleSnapshot() == null ? voucher.getTitle() : order.getTitleSnapshot());
    }
    return Result.ok(detail);
  }

  @Override
  public Result queryMyVoucherOrder(Long voucherId) {
    VoucherOrder order =
        query()
            .eq("user_id", UserHolder.getUser().getId())
            .eq("voucher_id", voucherId)
            .orderByDesc("create_time")
            .last("LIMIT 1")
            .one();
    return Result.ok(order);
  }

  @Override
  public Result queryMyOrders(Integer page, boolean heldOnly) {
    if (page == null || page < 1 || page > 10000) {
      return Result.fail("分页参数无效");
    }
    LambdaQueryChainWrapper<VoucherOrder> query =
        lambdaQuery().eq(VoucherOrder::getUserId, UserHolder.getUser().getId());
    if (heldOnly) {
      query.eq(VoucherOrder::getStatus, OrderStatus.PAID.getCode());
    }
    Page<VoucherOrder> result =
        query
            .orderByDesc(VoucherOrder::getCreateTime)
            .orderByDesc(VoucherOrder::getId)
            .page(new Page<>(page, 20));
    Map<Long, Voucher> vouchers =
        result.getRecords().isEmpty()
            ? Collections.emptyMap()
            : voucherService
                .listByIds(
                    result.getRecords().stream()
                        .map(VoucherOrder::getVoucherId)
                        .collect(Collectors.toSet()))
                .stream()
                .collect(Collectors.toMap(Voucher::getId, v -> v));
    List<Map<String, Object>> items = new ArrayList<>();
    for (VoucherOrder order : result.getRecords()) {
      Voucher voucher = vouchers.get(order.getVoucherId());
      Map<String, Object> item = new HashMap<>();
      item.put("order_id", order.getId().toString());
      item.put("voucher_id", order.getVoucherId());
      item.put("status", order.getStatus());
      item.put("created_at", order.getCreateTime());
      if (voucher != null) {
        item.put("shop_id", voucher.getShopId());
        item.put(
            "title",
            order.getTitleSnapshot() == null ? voucher.getTitle() : order.getTitleSnapshot());
        item.put(
            "pay_cents",
            order.getPayValueSnapshot() == null
                ? voucher.getPayValue()
                : order.getPayValueSnapshot());
        item.put(
            "voucher_type",
            order.getVoucherTypeSnapshot() == null
                ? voucher.getType()
                : order.getVoucherTypeSnapshot());
        item.put(
            "rules",
            order.getRulesSnapshot() == null ? voucher.getRules() : order.getRulesSnapshot());
      }
      items.add(item);
    }
    return Result.ok(items, result.getTotal());
  }

  @Override
  public Result payMockOrder(Long orderId, Long expectedPayCents) {
    if (!mockPaymentEnabled) {
      return Result.fail("本地模拟支付未启用");
    }
    return transactionTemplate.execute(
        tx -> {
          Long userId = UserHolder.getUser().getId();
          VoucherOrder order = getById(orderId);
          if (order == null || !userId.equals(order.getUserId())) {
            return Result.fail("订单不存在");
          }
          Voucher voucher = voucherService.getById(order.getVoucherId());
          Long payable =
              order.getPayValueSnapshot() == null
                  ? (voucher == null ? null : voucher.getPayValue())
                  : order.getPayValueSnapshot();
          if (payable == null || payable <= 0 || !payable.equals(expectedPayCents)) {
            return Result.fail("支付金额不匹配，请重新查询");
          }
          if (OrderStatus.PAID.matches(order.getStatus())) {
            return Result.ok(orderId.toString());
          }
          if (!OrderStatus.UNPAID.matches(order.getStatus())) {
            return Result.fail("订单不是待支付状态");
          }
          boolean changed =
              update()
                  .set("status", OrderStatus.PAID.getCode())
                  .set("pay_time", LocalDateTime.now())
                  .eq("id", orderId)
                  .eq("user_id", userId)
                  .eq("status", OrderStatus.UNPAID.getCode())
                  .apply("(expire_at IS NULL OR expire_at > NOW(3))")
                  .update();
          if (!changed) {
            VoucherOrder latest = getById(orderId);
            if (latest == null
                || !userId.equals(latest.getUserId())
                || !OrderStatus.PAID.matches(latest.getStatus())) {
              return Result.fail("订单状态已变化，请刷新后重试");
            }
          }
          if (changed) {
            lifecycle.registerOrderPaidEvent(orderId, userId);
          }
          return Result.ok(orderId.toString());
        });
  }

  @Override
  public Result cancelUnpaidOrdinaryOrder(Long orderId) {
    return transactionTemplate.execute(
        tx -> {
          Long userId = UserHolder.getUser().getId();
          VoucherOrder order = getById(orderId);
          if (order == null || !userId.equals(order.getUserId())) {
            return Result.fail("订单不存在");
          }
          Voucher voucher = voucherService.getById(order.getVoucherId());
          if (voucher == null
              || voucher.getType() == null
              || !VoucherType.ORDINARY.matches(voucher.getType())
              || voucher.getPayValue() == null
              || voucher.getPayValue() <= 0) {
            return Result.fail("仅支持取消未支付的普通券订单");
          }
          if (OrderStatus.CANCELLED.matches(order.getStatus())) {
            return Result.ok(orderId);
          }
          if (!OrderStatus.UNPAID.matches(order.getStatus())) {
            return Result.fail("订单不是待支付状态");
          }
          boolean changed =
              update()
                  .set("status", OrderStatus.CANCELLED.getCode())
                  .eq("id", orderId)
                  .eq("user_id", userId)
                  .eq("status", OrderStatus.UNPAID.getCode())
                  .update();
          if (changed) {
            lifecycle.registerOrderClosedEvent(orderId, userId);
          }
          return changed ? Result.ok(orderId) : Result.fail("订单状态已变化，请刷新后重试");
        });
  }

  private static int initialOrderStatus(Voucher voucher) {
    boolean freeVoucher = voucher.getPayValue() != null && voucher.getPayValue() == 0L;
    return freeVoucher ? OrderStatus.PAID.getCode() : OrderStatus.UNPAID.getCode();
  }

  private static String admissionMetricLabel(Long result) {
    if (result == null) {
      return "unavailable";
    }
    if (result == 0) {
      return "accepted";
    }
    if (result == 1) {
      return "sold_out";
    }
    if (result == 2) {
      return "duplicate";
    }
    return result == 3 ? "inactive" : "invalid";
  }

  private static void snapshot(VoucherOrder order, Voucher voucher) {
    order.setShopIdSnapshot(voucher.getShopId());
    order.setPayValueSnapshot(voucher.getPayValue());
    order.setTitleSnapshot(voucher.getTitle());
    order.setRulesSnapshot(voucher.getRules());
    order.setVoucherTypeSnapshot(voucher.getType());
  }
}
