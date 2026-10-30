package com.hmdp.dto;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** 统一业务响应；泛型只约束 Java 内部类型，JSON 字段保持不变。 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Result<T> {
  private Boolean success;
  private String errorMsg;
  private T data;
  private Long total;

  public static Result<Void> ok() {
    return new Result<>(true, null, null, null);
  }

  public static <T> Result<T> ok(T data) {
    return new Result<>(true, null, data, null);
  }

  public static <T> Result<List<T>> ok(List<T> data, Long total) {
    return new Result<>(true, null, data, total);
  }

  public static <T> Result<T> fail(String errorMsg) {
    return new Result<>(false, errorMsg, null, null);
  }
}
