package com.hmdp.service;

import com.hmdp.dto.Result;

public interface IShopHeatService {
  void refresh();

  Result ranking(int windowMinutes, int limit);
}
