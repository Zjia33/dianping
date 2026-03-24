package org.javaup.ratelimit.annotation;

import org.javaup.ratelimit.extension.RateLimitScene;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 限流注解
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RateLimit {

    /**
     * 限流场景
     */
    RateLimitScene scene();

    /**
     * 优惠券ID参数名（支持SpEL表达式，如 "#voucherId" 或 "#voucher.id"）
     * 默认从方法参数中查找名为 "id" 的参数
     */
    String voucherIdParam() default "id";

    /**
     * 用户ID参数名（支持SpEL表达式）
     * 默认为空，表示从 UserHolder 获取
     */
    String userIdParam() default "";
}