package org.javaup.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.javaup.entity.RollbackFailureLog;
import org.javaup.mapper.RollbackFailureLogMapper;
import org.javaup.service.IRollbackFailureLogService;
import org.springframework.stereotype.Service;

/**
 * @description: 回滚失败日志 接口实现
 */
@Service
public class RollbackFailureLogServiceImpl extends ServiceImpl<RollbackFailureLogMapper, RollbackFailureLog>
        implements IRollbackFailureLogService {
}