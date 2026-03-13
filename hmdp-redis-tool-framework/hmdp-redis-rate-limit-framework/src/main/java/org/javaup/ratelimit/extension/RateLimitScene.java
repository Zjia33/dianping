package org.javaup.ratelimit.extension;

/**
 * @description: 限流场景
 */
public enum RateLimitScene {
    /** 发令牌接口 */
    ISSUE_TOKEN,
    /** 下单（秒杀）接口 */
    SECKILL_ORDER
}