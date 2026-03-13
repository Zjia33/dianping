package org.javaup.service;

/**
 * @description: 对账执行 接口
 */
public interface IReconciliationTaskService {
    
    void reconciliationTaskExecute();

    /**
     * 删除指定券的 Redis 库存键，触发按需重载。
     */
    void delRedisStock(Long voucherId);
}
