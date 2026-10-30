package com.hmdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.dto.Result;
import com.hmdp.dto.ShopSearchDTO;
import com.hmdp.entity.Shop;
import java.util.List;

/**
 * 服务类
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IShopService extends IService<Shop> {

  Result queryById(Long id);

  Result update(Shop shop);

  Result queryShopByType(Integer typeId, Integer current, Double x, Double y);

  Result searchShops(ShopSearchDTO criteria);

  Result catalog(String keyword, Long typeId, Integer page, Integer size, String sort);

  Result queryShopsByIds(List<Long> ids);

  Result listAreas();

  Result saveShop(Shop shop);

  Result queryShopByName(String name, Integer current);

  Result queryShopDetail(Long id);
}
