package com.hmdp.controller;

import com.hmdp.dto.Result;
import com.hmdp.dto.ShopSearchDTO;
import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import java.util.List;
import javax.annotation.Resource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 前端控制器
 *
 * @author 虎哥
 */
@RestController
@RequestMapping("/shop")
public class ShopController {

  @Resource
  private IShopService shopService;
  /**
   * 根据id查询商铺信息
   *
   * @param id 商铺id
   * @return 商铺详情数据
   */
  @GetMapping("/{id}")
  public Result queryShopById(@PathVariable("id") Long id) {
    return shopService.queryShopDetail(id);
  }

  /** 供 CityLens 使用的只读组合搜索，评分参数按数据库的 10 倍整数传入。 */
  @GetMapping("/search")
  public Result searchShops(ShopSearchDTO criteria) {
    return shopService.searchShops(criteria);
  }

  /** 统一前端的跨分类店铺列表；Agent 的严格搜索接口保持原有条件。 */
  @GetMapping("/catalog")
  public Result catalog(
      @RequestParam(value = "keyword", defaultValue = "") String keyword,
      @RequestParam(value = "typeId", required = false) Long typeId,
      @RequestParam(value = "page", defaultValue = "1") Integer page,
      @RequestParam(value = "size", defaultValue = "12") Integer size,
      @RequestParam(value = "sort", defaultValue = "score") String sort) {
    return shopService.catalog(keyword, typeId, page, size, sort);
  }

  /** CityLens 在组合两站前批量复核候选，直接读取当前店铺数据。 */
  @GetMapping("/batch")
  public Result queryShopsByIds(@RequestParam("ids") List<Long> ids) {
    return shopService.queryShopsByIds(ids);
  }

  /** 供 CityLens 在用户原话中匹配真实商圈，避免猜测地名。 */
  @GetMapping("/areas")
  public Result listAreas() {
    return shopService.listAreas();
  }

  /**
   * 新增商铺信息
   *
   * @param shop 商铺数据
   * @return 商铺id
   */
  @PostMapping
  public Result saveShop(@RequestBody Shop shop) {
    return shopService.saveShop(shop);
  }

  /**
   * 更新商铺信息
   *
   * @param shop 商铺数据
   * @return 无
   */
  @PutMapping
  public Result updateShop(@RequestBody Shop shop) {
    // 写入数据库
    return shopService.update(shop);
  }

  /**
   * 根据商铺类型分页查询商铺信息
   *
   * @param typeId 商铺类型
   * @param current 页码
   * @return 商铺列表
   */
  @GetMapping("/of/type")
  public Result queryShopByType(
      @RequestParam("typeId") Integer typeId,
      @RequestParam(value = "current", defaultValue = "1") Integer current,
      @RequestParam(value = "x", required = false) Double x,
      @RequestParam(value = "y", required = false) Double y) {
    return shopService.queryShopByType(typeId, current, x, y);
  }

  /**
   * 根据商铺名称关键字分页查询商铺信息
   *
   * @param name 商铺名称关键字
   * @param current 页码
   * @return 商铺列表
   */
  @GetMapping("/of/name")
  public Result queryShopByName(
      @RequestParam(value = "name", required = false) String name,
      @RequestParam(value = "current", defaultValue = "1") Integer current) {
    return shopService.queryShopByName(name, current);
  }
}
