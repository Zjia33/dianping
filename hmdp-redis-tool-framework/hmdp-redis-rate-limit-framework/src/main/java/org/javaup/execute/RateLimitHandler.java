package org.javaup.execute;
import org.javaup.ratelimit.extension.RateLimitScene;

/**
 * @description: 限流执行 接口
 */
public interface RateLimitHandler {
   
    void execute(Long voucherId, Long userId, RateLimitScene scene);
}
