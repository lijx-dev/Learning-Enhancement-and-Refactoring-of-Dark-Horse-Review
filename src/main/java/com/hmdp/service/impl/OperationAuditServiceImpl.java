package com.hmdp.service.impl;

import com.hmdp.mapper.OperationAuditMapper;
import com.hmdp.service.IOperationAuditService;
import javax.annotation.Resource;
import org.springframework.stereotype.Service;

@Service
public class OperationAuditServiceImpl implements IOperationAuditService {
  @Resource
  private OperationAuditMapper operationAuditMapper;

  @Override
  public void recordAudit(Long userId, String method, String path, Integer httpStatus) {
    operationAuditMapper.insertAudit(userId, method, path, httpStatus);
  }
}
