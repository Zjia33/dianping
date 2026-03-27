package org.javaup.rabbitmq.consumer;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.javaup.cache.SeckillVoucherLocalCache;
import org.javaup.consumer.rabbit.AbstractRabbitConsumerHandler;
import org.javaup.core.RedisKeyManage;
import org.javaup.kafka.message.SeckillVoucherInvalidationMessage;
import org.javaup.message.MessageExtend;
import org.javaup.rabbitmq.config.RabbitMQConfig;
import org.javaup.redis.RedisCache;
import org.javaup.redis.RedisKeyBuild;
import org.javaup.servicelock.LockType;
import org.javaup.servicelock.annotion.ServiceLock;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.aop.framework.AopContext;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;

import static org.javaup.constant.DistributedLockConstants.UPDATE_SECKILL_VOUCHER_LOCK;

/**
 * @description: RabbitMQ 消费者：接收"秒杀券缓存失效"广播
 */
@Slf4j
@Component
public class RabbitSeckillVoucherInvalidationConsumer extends AbstractRabbitConsumerHandler<SeckillVoucherInvalidationMessage> {

    @Resource
    private SeckillVoucherLocalCache seckillVoucherLocalCache;

    @Resource
    private MeterRegistry meterRegistry;

    @Resource
    private RedisCache redisCache;

    public RabbitSeckillVoucherInvalidationConsumer() {
        super(SeckillVoucherInvalidationMessage.class);
    }

    @RabbitListener(queues = RabbitMQConfig.SECKILL_VOUCHER_INVALIDATION_QUEUE)
    public void onMessage(Message message, Map<String, Object> headers) {
        String body = new String(message.getBody());
        String key = (String) headers.get("spring_rabbitmq_routingKey");
        consumeRaw(body, key, headers);
    }

    @Override
    protected void doConsume(MessageExtend<SeckillVoucherInvalidationMessage> message) {
        SeckillVoucherInvalidationMessage body = message.getMessageBody();
        if (Objects.isNull(body.getVoucherId())) {
            log.warn("收到缓存失效消息但载荷为空或voucherId缺失, uuid={}", message.getUuid());
            return;
        }
        Long voucherId = body.getVoucherId();

        ((RabbitSeckillVoucherInvalidationConsumer) AopContext.currentProxy()).delCache(voucherId);
    }

    @ServiceLock(lockType = LockType.Write, name = UPDATE_SECKILL_VOUCHER_LOCK, keys = {"#voucherId"})
    public void delCache(Long voucherId) {
        RedisKeyBuild seckillVoucherRedisKey =
                RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_VOUCHER_TAG_KEY, voucherId);
        seckillVoucherLocalCache.invalidate(seckillVoucherRedisKey.getRelKey());

        redisCache.del(RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_VOUCHER_TAG_KEY, voucherId));
        redisCache.del(RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_STOCK_TAG_KEY, voucherId));
        redisCache.del(RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_VOUCHER_NULL_TAG_KEY, voucherId));
    }

    @Override
    protected void afterConsumeFailure(final MessageExtend<SeckillVoucherInvalidationMessage> message, final Throwable throwable) {
        super.afterConsumeFailure(message, throwable);
        log.warn("删除Redis缓存失败 voucherId={}", message.getMessageBody().getVoucherId(), throwable);
        safeInc(errorTag(throwable));
    }

    private void safeInc(String tagValue) {
        try {
            if (meterRegistry != null) {
                meterRegistry.counter("seckill_invalidation_consume_failures", "error", tagValue).increment();
            }
        } catch (Exception ignore) {
        }
    }

    private String errorTag(Throwable t) {
        return t == null ? "unknown" : t.getClass().getSimpleName();
    }
}
