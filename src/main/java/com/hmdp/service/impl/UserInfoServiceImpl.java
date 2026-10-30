package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.UserInfo;
import com.hmdp.mapper.UserInfoMapper;
import com.hmdp.service.IUserInfoService;
import org.springframework.stereotype.Service;

/**
 * 服务实现类
 *
 * @author 虎哥
 * @since 2021-12-24
 */
@Service
public class UserInfoServiceImpl extends ServiceImpl<UserInfoMapper, UserInfo>
    implements IUserInfoService {

  @Override
  public Result queryUserInfo(Long userId) {
    UserInfo info = getById(userId);
    if (info == null) {
      return Result.ok();
    }
    info.setCreateTime(null);
    info.setUpdateTime(null);
    return Result.ok(info);
  }
}
