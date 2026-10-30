package com.hmdp.service;

public interface IOperationAuditService {
  void recordAudit(Long userId, String method, String path, Integer httpStatus);
}
