package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.Result;
import com.hmdp.mapper.MediaMapper;
import com.hmdp.reliability.MediaStorage;
import com.hmdp.service.IBusinessOutboxService;
import com.hmdp.service.IMediaService;
import com.hmdp.utils.SystemConstants;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import javax.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.multipart.MultipartFile;

@Slf4j
@Service
public class MediaServiceImpl implements IMediaService {

  @Resource
  private MediaMapper mediaMapper;

  private final TransactionTemplate transactions;
  private final IBusinessOutboxService outbox;

  public MediaServiceImpl(TransactionTemplate transactions, IBusinessOutboxService outbox) {
    this.transactions = transactions;
    this.outbox = outbox;
  }

  private SortedSet<String> paths(String images) {
    SortedSet<String> paths = new TreeSet<>();
    for (String path : images.split(",")) {
      if (!path.isEmpty()) {
        paths.add(MediaStorage.normalize(path));
      }
    }
    return paths;
  }

  @Override
  public boolean lockOwnedImages(String images, long user) {
    if (!TransactionSynchronizationManager.isActualTransactionActive()) {
      throw new IllegalStateException("图片引用需要业务事务");
    }
    try {
      // All publishers lock paths in the same order; deletion locks the same owner row.
      for (String path : paths(images)) {
        List<Map<String, Object>> rows = mediaMapper.selectOwnerForUpdate(path);
        if (rows.isEmpty()
            || ((Number) rows.get(0).get("user_id")).longValue() != user
            || !"ACTIVE".equals(rows.get(0).get("state"))) {
          return false;
        }
      }
      return true;
    } catch (IllegalArgumentException invalid) {
      return false;
    }
  }

  @Override
  public void attach(long blog, String images) {
    if (!TransactionSynchronizationManager.isActualTransactionActive()) {
      throw new IllegalStateException("图片引用需要业务事务");
    }
    for (String path : paths(images)) {
      mediaMapper.insertBlogReference(blog, path);
    }
  }

  @Override
  public Result delete(String input, long user) {
    final String path;
    try {
      path = MediaStorage.normalize(input);
    } catch (IllegalArgumentException invalid) {
      return Result.fail(invalid.getMessage());
    }
    return transactions.execute(
        tx -> {
          List<Map<String, Object>> rows = mediaMapper.selectOwnerForUpdate(path);
          if (rows.isEmpty() || ((Number) rows.get(0).get("user_id")).longValue() != user) {
            return Result.fail("图片不存在或无权删除");
          }
          if (!"ACTIVE".equals(rows.get(0).get("state"))) {
            return Result.ok();
          }
          if (!mediaMapper.selectReferencesForUpdate(path).isEmpty()) {
            return Result.fail("图片已用于笔记，不能直接删除");
          }
          mediaMapper.markDeleting(path);
          outbox.appendMediaDeletionEvent(path, user);
          return Result.ok();
        });
  }

  @Value("${hmdp.upload-dir:" + SystemConstants.IMAGE_UPLOAD_DIR + "}")
  private String uploadDir;

  @Override
  public Result upload(MultipartFile image, long user) {
    try {
      // 获取原始文件名称
      String originalFilename = image.getOriginalFilename();
      if (image.isEmpty()
          || image.getSize() > 5000000
          || originalFilename == null
          || !originalFilename.toLowerCase().matches(".*\\.(jpg|jpeg|png|webp)$")) {
        return Result.fail("图片类型或大小不合法");
      }
      byte[] signature = new byte[12];
      try (InputStream stream = image.getInputStream()) {
        if (stream.read(signature) < 12) {
          return Result.fail("图片内容不合法");
        }
      }
      String lower = originalFilename.toLowerCase(Locale.ROOT);
      boolean valid =
          (lower.endsWith(".png")
                  && signature[0] == (byte) 137
                  && signature[1] == 80
                  && signature[2] == 78
                  && signature[3] == 71)
              || ((lower.endsWith(".jpg") || lower.endsWith(".jpeg"))
                  && signature[0] == (byte) 255
                  && signature[1] == (byte) 216
                  && signature[2] == (byte) 255)
              || (lower.endsWith(".webp")
                  && new String(signature, 0, 4, StandardCharsets.US_ASCII).equals("RIFF")
                  && new String(signature, 8, 4, StandardCharsets.US_ASCII).equals("WEBP"));
      if (!valid) {
        return Result.fail("文件内容与图片类型不匹配");
      }
      // 生成新文件名
      String fileName = createNewFileName(lower);
      // 保存文件
      image.transferTo(new File(uploadDir, fileName));
      mediaMapper.insertOwner(fileName, user);
      // 返回结果
      log.debug("文件上传成功，{}", fileName);
      return Result.ok(fileName);
    } catch (IOException e) {
      throw new RuntimeException("文件上传失败", e);
    }
  }

  private String createNewFileName(String originalFilename) {
    // 获取后缀
    String suffix = StrUtil.subAfter(originalFilename, ".", true);
    // 生成目录
    String name = UUID.randomUUID().toString();
    int hash = name.hashCode();
    int d1 = hash & 0xF;
    int d2 = (hash >> 4) & 0xF;
    // 判断目录是否存在
    File dir = new File(uploadDir, StrUtil.format("/blogs/{}/{}", d1, d2));
    if (!dir.exists()) {
      dir.mkdirs();
    }
    // 生成文件名
    return StrUtil.format("/blogs/{}/{}/{}.{}", d1, d2, name, suffix);
  }
}
