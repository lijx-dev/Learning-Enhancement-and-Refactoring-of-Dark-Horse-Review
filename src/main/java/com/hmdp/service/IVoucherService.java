package com.hmdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.dto.Result;
import com.hmdp.entity.Voucher;

/**
 * 服务类
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherService extends IService<Voucher> {

  Result queryVoucherOfShop(Long shopId);

  void addSeckillVoucher(Voucher voucher);

  Result capabilities();

  Result createSeckillVoucher(Voucher voucher);

  Result createVoucher(Voucher voucher);
}
