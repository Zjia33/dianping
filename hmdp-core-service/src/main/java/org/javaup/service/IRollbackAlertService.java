package org.javaup.service;

import org.javaup.entity.RollbackFailureLog;

/**
 * @description: 回滚失败通知服务：用于发送短信/邮件告警（可插拔实现）。
 */
public interface IRollbackAlertService {

    void sendRollbackAlert(RollbackFailureLog log);
}