package com.hmdp.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hmdp.entity.Voucher;
import java.util.List;
import org.apache.ibatis.annotations.Param;

/**
 * Mapper 接口
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface VoucherMapper extends BaseMapper<Voucher> {

  List<Voucher> queryVoucherOfShop(@Param("shopId") Long shopId);

  List<Long> selectActiveVoucherIds(@Param("shopId") Long shopId);
}
