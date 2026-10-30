package com.hmdp.controller;

import com.hmdp.dto.Result;
import com.hmdp.entity.Voucher;
import com.hmdp.service.IVoucherService;
import javax.annotation.Resource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 前端控制器
 *
 * @author 虎哥
 */
@RestController
@RequestMapping("/voucher")
public class VoucherController {

  @Resource
  private IVoucherService voucherService;

  @GetMapping("/manage/capabilities")
  public Result capabilities() {
    return voucherService.capabilities();
  }

  /**
   * 新增秒杀券
   *
   * @param voucher 优惠券信息，包含秒杀信息
   * @return 优惠券id
   */
  @PostMapping("seckill")
  public Result addSeckillVoucher(@RequestBody Voucher voucher) {
    return voucherService.createSeckillVoucher(voucher);
  }

  /**
   * 新增普通券
   *
   * @param voucher 优惠券信息
   * @return 优惠券id
   */
  @PostMapping
  public Result addVoucher(@RequestBody Voucher voucher) {
    return voucherService.createVoucher(voucher);
  }

  /**
   * 查询店铺的优惠券列表
   *
   * @param shopId 店铺id
   * @return 优惠券列表
   */
  @GetMapping("/list/{shopId}")
  public Result queryVoucherOfShop(@PathVariable("shopId") Long shopId) {
    return voucherService.queryVoucherOfShop(shopId);
  }
}
