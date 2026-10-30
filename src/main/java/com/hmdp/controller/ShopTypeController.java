package com.hmdp.controller;

import com.hmdp.dto.Result;
import com.hmdp.service.IShopTypeService;
import javax.annotation.Resource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 前端控制器
 *
 * @author 虎哥
 */
@RestController
@RequestMapping("/shop-type")
public class ShopTypeController {
  @Resource
  private IShopTypeService typeService;

  @GetMapping("list")
  public Result queryTypeList() {
    return typeService.queryTypeList();
  }
}
