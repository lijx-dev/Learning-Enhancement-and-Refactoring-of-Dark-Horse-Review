package com.hmdp.controller;

import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.service.IUserInfoService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 前端控制器
 *
 * @author 虎哥
 */
@Slf4j
@RestController
@RequestMapping("/user")
public class UserController {

  @Resource
  private IUserService userService;

  @Resource
  private IUserInfoService userInfoService;

  /** 发送手机验证码 */
  @PostMapping("code")
  public Result sendCode(@RequestParam("phone") String phone, HttpSession session) {
    // 发送短信验证码并保存验证码
    return userService.sendCode(phone, session);
  }

  /**
   * 登录功能
   *
   * @param loginForm 登录参数，包含手机号、验证码；或者手机号、密码
   */
  @PostMapping("/login")
  public Result login(@RequestBody LoginFormDTO loginForm, HttpSession session) {
    // 实现登录功能
    return userService.login(loginForm, session);
  }

  /**
   * 登出功能
   *
   * @return 无
   */
  @PostMapping("/logout")
  public Result logout(@RequestHeader(value = "authorization", required = false) String token) {
    return userService.logout(token);
  }

  @GetMapping("/me")
  public Result me() {
    // 获取当前登录的用户并返回
    UserDTO user = UserHolder.getUser();
    return Result.ok(user);
  }

  @GetMapping("/info/{id}")
  public Result info(@PathVariable("id") Long userId) {
    return userInfoService.queryUserInfo(userId);
  }

  @GetMapping("/{id}")
  public Result queryUserById(@PathVariable("id") Long userId) {
    return userService.queryUserById(userId);
  }

  @PostMapping("/sign")
  public Result signInToday() {
    return userService.signInToday();
  }

  @GetMapping("/sign/count")
  public Result queryCurrentMonthConsecutiveSignInDays() {
    return userService.queryCurrentMonthConsecutiveSignInDays();
  }
}
