package com.hmdp.controller;

import com.hmdp.dto.Result;
import com.hmdp.service.IExperienceService;
import java.util.List;
import javax.annotation.Resource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/experience")
public class ExperienceController {
  @Resource
  private IExperienceService experienceService;

  @GetMapping("/capabilities")
  public Result queryExperienceCapabilities() {
    return experienceService.queryExperienceCapabilities();
  }

  @GetMapping("/cache")
  public Result traceShopCacheReads(@RequestParam Long shopId) {
    return experienceService.traceShopCacheReads(shopId);
  }

  @PostMapping("/visits")
  public Result submitDemoVisitEvents(@RequestParam Long shopId) {
    return experienceService.submitDemoVisitEvents(shopId);
  }

  @GetMapping("/visits")
  public Result queryVisitEventReceipts(@RequestParam List<String> eventIds) {
    return experienceService.queryVisitEventReceipts(eventIds);
  }

  @GetMapping("/orders/{id}")
  public Result queryMyOrderProgress(@PathVariable("id") Long orderId) {
    return experienceService.queryMyOrderProgress(orderId);
  }
}
