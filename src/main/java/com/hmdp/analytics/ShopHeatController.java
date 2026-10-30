package com.hmdp.analytics;

import com.hmdp.dto.Result;
import com.hmdp.service.IShopHeatService;
import javax.annotation.Resource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/shop/hot-ranking")
public class ShopHeatController {
  @Resource
  private IShopHeatService shopHeatService;

  @GetMapping
  public Result ranking(
      @RequestParam(defaultValue = "60") int windowMinutes,
      @RequestParam(defaultValue = "10") int limit) {
    return shopHeatService.ranking(windowMinutes, limit);
  }
}
