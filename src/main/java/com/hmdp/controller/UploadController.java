package com.hmdp.controller;

import com.hmdp.dto.Result;
import com.hmdp.service.IMediaService;
import com.hmdp.utils.UserHolder;
import javax.annotation.Resource;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("upload")
public class UploadController {
  @Resource
  private IMediaService mediaService;

  @PostMapping("blog")
  public Result uploadImage(@RequestParam("file") MultipartFile image) {
    return mediaService.upload(image, UserHolder.getUser().getId());
  }

  @GetMapping("/blog/delete")
  public Result deleteBlogImgLegacy(@RequestParam("name") String filename) {
    return mediaService.delete(filename, UserHolder.getUser().getId());
  }

  @DeleteMapping("/blog")
  public Result deleteBlogImg(@RequestParam("name") String filename) {
    return mediaService.delete(filename, UserHolder.getUser().getId());
  }
}
