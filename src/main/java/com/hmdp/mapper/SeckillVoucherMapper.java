package com.hmdp.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hmdp.entity.SeckillPublication;
import com.hmdp.entity.SeckillVoucher;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.annotations.Param;

/**
 * 秒杀优惠券表，与优惠券是一对一关系 Mapper 接口
 *
 * @author 虎哥
 * @since 2022-01-04
 */
public interface SeckillVoucherMapper extends BaseMapper<SeckillVoucher> {

  List<Map<String, Object>> selectPublicationStates();

  int markPublished(@Param("voucherId") Long voucherId);

  SeckillPublication selectPublicationDetails(@Param("voucherId") Long voucherId);

  Integer selectPublishedFlag(@Param("voucherId") Long voucherId);

  List<Map<String, Object>> selectInventory(@Param("voucherId") Long voucherId);

  List<Long> selectActivityVoucherIds();

  int insertActivity(@Param("voucherId") Long voucherId, @Param("totalStock") Integer totalStock);
}
