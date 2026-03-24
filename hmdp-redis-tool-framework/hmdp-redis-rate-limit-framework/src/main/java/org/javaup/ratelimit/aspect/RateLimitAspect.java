package org.javaup.ratelimit.aspect;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.javaup.execute.RateLimitHandler;
import org.javaup.ratelimit.annotation.RateLimit;
import org.javaup.ratelimit.extension.RateLimitScene;
import org.springframework.core.annotation.Order;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * 限流切面
 */
@Slf4j
@Aspect
@Order(-10)
@AllArgsConstructor
public class RateLimitAspect {

    private final RateLimitHandler rateLimitHandler;

    private final ExpressionParser parser = new SpelExpressionParser();

    @Around("@annotation(rateLimit)")
    public Object around(ProceedingJoinPoint joinPoint, RateLimit rateLimit) throws Throwable {
        RateLimitScene scene = rateLimit.scene();

        // 获取 voucherId
        Long voucherId = resolveVoucherId(joinPoint, rateLimit.voucherIdParam());

        // 获取 userId
        Long userId = resolveUserId(joinPoint, rateLimit.userIdParam());

        // 执行限流
        rateLimitHandler.execute(voucherId, userId, scene);

        return joinPoint.proceed();
    }

    private Long resolveVoucherId(ProceedingJoinPoint joinPoint, String voucherIdParam) {
        // SpEL 表达式解析
        if (voucherIdParam.startsWith("#")) {
            MethodSignature signature = (MethodSignature) joinPoint.getSignature();
            String[] parameterNames = signature.getParameterNames();
            Object[] args = joinPoint.getArgs();

            StandardEvaluationContext context = new StandardEvaluationContext();
            for (int i = 0; i < parameterNames.length; i++) {
                context.setVariable(parameterNames[i], args[i]);
            }

            Expression expression = parser.parseExpression(voucherIdParam);
            Object result = expression.getValue(context);
            if (result instanceof Number) {
                return ((Number) result).longValue();
            }
            return Long.parseLong(result.toString());
        }

        // 直接解析路径变量（如 /{id}）
        if ("id".equals(voucherIdParam)) {
            for (Object arg : joinPoint.getArgs()) {
                if (arg instanceof Long) {
                    return (Long) arg;
                }
            }
        }

        return null;
    }

    private Long resolveUserId(ProceedingJoinPoint joinPoint, String userIdParam) {
        // SpEL 表达式解析
        if (userIdParam != null && !userIdParam.isEmpty()) {
            if (userIdParam.startsWith("#")) {
                MethodSignature signature = (MethodSignature) joinPoint.getSignature();
                String[] parameterNames = signature.getParameterNames();
                Object[] args = joinPoint.getArgs();

                StandardEvaluationContext context = new StandardEvaluationContext();
                for (int i = 0; i < parameterNames.length; i++) {
                    context.setVariable(parameterNames[i], args[i]);
                }

                Expression expression = parser.parseExpression(userIdParam);
                Object result = expression.getValue(context);
                if (result instanceof Number) {
                    return ((Number) result).longValue();
                }
                return Long.parseLong(result.toString());
            }
        }

        // 默认通过反射调用 UserHolder.getUser().getId()
        try {
            Class<?> userHolderClass = Class.forName("org.javaup.utils.UserHolder");
            Object user = userHolderClass.getMethod("getUser").invoke(null);
            if (user != null) {
                Object id = user.getClass().getMethod("getId").invoke(user);
                if (id instanceof Number) {
                    return ((Number) id).longValue();
                }
                return Long.parseLong(id.toString());
            }
        } catch (ClassNotFoundException e) {
            log.debug("UserHolder class not found, please specify userIdParam in @RateLimit annotation");
        } catch (Exception e) {
            log.warn("Failed to get userId from UserHolder", e);
        }
        return null;
    }
}