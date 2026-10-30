package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.cache.VoucherCatalog;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.service.IBusinessOutboxService;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IShopService;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.UserHolder;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Resource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 服务实现类
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherServiceImpl extends ServiceImpl<VoucherMapper, Voucher>
    implements IVoucherService {

  @Resource
  private SeckillVoucherMapper seckillVoucherMapper;

  @Resource
  private ISeckillVoucherService seckillVoucherService;

  @Resource
  private StringRedisTemplate stringRedisTemplate;

  @Resource
  private IBusinessOutboxService outbox;

  @Resource
  private CacheClient cache;

  @Override
  public Result queryVoucherOfShop(Long shopId) {
    VoucherCatalog catalog =
        cache.queryWithMutex(
            "cache:voucher-list:",
            shopId,
            VoucherCatalog.class,
            id -> {
              VoucherCatalog value = new VoucherCatalog();
              value.setIds(baseMapper.selectActiveVoucherIds(id));
              return value;
            },
            10L,
            TimeUnit.MINUTES);
    List<Voucher> vouchers = new ArrayList<>();
    if (catalog == null || catalog.getIds() == null || catalog.getIds().isEmpty()) {
      return Result.ok(vouchers);
    }
    // Trading stock and time windows are always read live, in one batch rather than N queries.
    Map<Long, SeckillVoucher> activities = new HashMap<>();
    for (SeckillVoucher activity : seckillVoucherService.listByIds(catalog.getIds())) {
      activities.put(activity.getVoucherId(), activity);
    }
    LocalDateTime now = LocalDateTime.now();
    for (Long id : catalog.getIds()) {
      Voucher voucher =
          cache.queryWithMutex(
              "cache:voucher-meta:", id, Voucher.class, this::getById, 10L, TimeUnit.MINUTES);
      if (voucher == null
          || !Objects.equals(voucher.getStatus(), 1)
          || !Objects.equals(voucher.getShopId(), shopId)) {
        continue;
      }
      SeckillVoucher activity = activities.get(id);
      if (activity != null) {
        voucher.setStock(activity.getStock());
        voucher.setBeginTime(activity.getBeginTime());
        voucher.setEndTime(activity.getEndTime());
      }
      boolean active =
          Objects.equals(voucher.getType(), 0)
              || (Objects.equals(voucher.getType(), 1)
                  && voucher.getStock() != null
                  && voucher.getStock() > 0
                  && voucher.getBeginTime() != null
                  && voucher.getEndTime() != null
                  && !now.isBefore(voucher.getBeginTime())
                  && !now.isAfter(voucher.getEndTime()));
      voucher.setOrderable(active);
      vouchers.add(voucher);
    }
    // 返回结果
    return Result.ok(vouchers);
  }

  @Override
  @Transactional
  public void addSeckillVoucher(Voucher voucher) {
    // 保存优惠券
    save(voucher);
    // 保存秒杀信息
    SeckillVoucher seckillVoucher = new SeckillVoucher();
    seckillVoucher.setVoucherId(voucher.getId());
    seckillVoucher.setStock(voucher.getStock());
    seckillVoucher.setBeginTime(voucher.getBeginTime());
    seckillVoucher.setEndTime(voucher.getEndTime());
    seckillVoucherService.save(seckillVoucher);
    seckillVoucherMapper.insertActivity(voucher.getId(), voucher.getStock());
    outbox.appendBusinessEvent("SECKILL_PUBLISH", voucher.getId(), null);
  }

  @Resource
  private IShopService shopService;

  @Value("${citylens.catalog-admin-ids:}")
  private String catalogAdminIds;

  private boolean canManage() {
    return UserHolder.getUser() != null
        && Arrays.stream(catalogAdminIds.split(","))
            .map(String::trim)
            .anyMatch(id -> id.equals(UserHolder.getUser().getId().toString()));
  }

  private String validate(Voucher v, boolean seckill) {
    if (!canManage()) {
      return "当前账号没有演示商品管理权限";
    }
    if (v.getId() != null
        || v.getShopId() == null
        || v.getShopId() <= 0
        || shopService.getById(v.getShopId()) == null) {
      return "关联店铺不存在或参数无效";
    }
    if (v.getTitle() == null
        || v.getTitle().trim().isEmpty()
        || v.getTitle().length() > 100
        || v.getRules() == null
        || v.getRules().trim().isEmpty()
        || v.getRules().length() > 1000
        || (v.getSubTitle() != null && v.getSubTitle().length() > 200)) {
      return "标题或规则无效";
    }
    if (v.getPayValue() == null
        || v.getActualValue() == null
        || v.getPayValue() < 0
        || v.getActualValue() <= 0
        || v.getActualValue() > 10000000
        || v.getPayValue() > v.getActualValue()) {
      return "券金额无效";
    }
    if (seckill
        && (v.getStock() == null
            || v.getStock() <= 0
            || v.getStock() > 100000
            || v.getBeginTime() == null
            || v.getEndTime() == null
            || !v.getEndTime().isAfter(v.getBeginTime()))) {
      return "秒杀库存或时间无效";
    }
    v.setType(seckill ? 1 : 0);
    v.setStatus(1);
    v.setCreateTime(null);
    v.setUpdateTime(null);
    if (!v.getTitle().startsWith("【演示】")) {
      v.setTitle("【演示】" + v.getTitle().trim());
    }
    return v.getTitle().length() > 100 ? "标题过长" : null;
  }

  @Override
  public Result capabilities() {
    Map<String, Object> data = new HashMap<>();
    data.put("can_create_voucher", canManage());
    data.put("server_time", LocalDateTime.now().toString());
    data.put("server_zone", ZoneId.systemDefault().toString());
    return Result.ok(data);
  }

  @Override
  @Transactional
  public Result createSeckillVoucher(Voucher voucher) {
    String error = validate(voucher, true);
    if (error != null) {
      return Result.fail(error);
    }
    addSeckillVoucher(voucher);
    return Result.ok(voucher.getId());
  }

  @Override
  @Transactional
  public Result createVoucher(Voucher voucher) {
    String error = validate(voucher, false);
    if (error != null) {
      return Result.fail(error);
    }
    if (!save(voucher)) {
      return Result.fail("保存失败");
    }
    return Result.ok(voucher.getId());
  }
}
