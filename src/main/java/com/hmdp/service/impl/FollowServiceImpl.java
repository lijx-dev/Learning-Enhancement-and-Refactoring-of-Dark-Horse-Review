package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IBusinessOutboxService;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Resource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 服务实现类
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

  @Resource
  private IUserService userService;

  @Resource
  private TransactionTemplate transactions;

  @Resource
  private IBusinessOutboxService outbox;

  @Override
  public Result follow(Long target, Boolean enabled) {
    long user = UserHolder.getUser().getId();
    if (target == null || target == user || userService.getById(target) == null) {
      return Result.fail("关注对象无效");
    }
    return transactions.execute(
        tx -> {
          int changed =
              Boolean.TRUE.equals(enabled)
                  ? baseMapper.insertEdge(user, target)
                  : baseMapper.deleteEdge(user, target);
          if (changed > 0) {
            outbox.appendBusinessEvent("FOLLOW", target, user);
          }
          return Result.ok();
        });
  }

  @Override
  public Result isFollow(Long target) {
    return Result.ok(baseMapper.countEdge(UserHolder.getUser().getId(), target) > 0);
  }

  @Override
  public Result followCommons(Long other) {
    List<Long> ids = baseMapper.selectCommonFollowIds(UserHolder.getUser().getId(), other);
    if (ids.isEmpty()) {
      return Result.ok(Collections.emptyList());
    }
    return Result.ok(
        userService.listByIds(ids).stream()
            .map(u -> BeanUtil.copyProperties(u, UserDTO.class))
            .collect(Collectors.toList()));
  }
}
