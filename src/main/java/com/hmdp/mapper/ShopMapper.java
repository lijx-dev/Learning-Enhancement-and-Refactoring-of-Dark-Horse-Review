package com.hmdp.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hmdp.entity.Shop;
import org.apache.ibatis.annotations.Param;

/**
 * Mapper 接口
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface ShopMapper extends BaseMapper<Shop> {

  Long countByShopId(@Param("shopId") Long shopId);
}
