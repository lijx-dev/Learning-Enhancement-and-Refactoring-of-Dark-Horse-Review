package com.hmdp.service;

import com.hmdp.dto.Result;
import org.springframework.web.multipart.MultipartFile;

/** 图片上传、归属校验和引用生命周期。 */
public interface IMediaService {
  boolean lockOwnedImages(String images, long user);

  void attach(long blog, String images);

  Result delete(String input, long user);

  Result upload(MultipartFile image, long user);
}
