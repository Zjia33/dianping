package org.javaup.ratelimit.extension;

import org.javaup.enums.BaseCode;

/**
 * @description: 默认空实现
 */
public class NoOpRateLimitPenaltyPolicy implements RateLimitPenaltyPolicy {
    @Override
    public void apply(RateLimitContext ctx, BaseCode reason) {
        // no-op
    }
}