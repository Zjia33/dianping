package org.javaup.rabbitmq.consumer;

import com.alibaba.fastjson.JSON;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.javaup.consumer.rabbit.AbstractRabbitConsumerHandler;
import org.javaup.kafka.message.SeckillVoucherInvalidationMessage;
import org.javaup.message.MessageExtend;
import org.javaup.rabbitmq.config.RabbitMQConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;

/**
 * @description: 秒杀券缓存失效广播的 DLQ 消费者
 */
@Slf4j
@Component
public class RabbitSeckillVoucherInvalidationDlqConsumer extends AbstractRabbitConsumerHandler<SeckillVoucherInvalidationMessage> {

    @Resource
    private MeterRegistry meterRegistry;

    private static final Logger auditLog = LoggerFactory.getLogger("AUDIT");

    public RabbitSeckillVoucherInvalidationDlqConsumer() {
        super(SeckillVoucherInvalidationMessage.class);
    }

    @RabbitListener(queues = RabbitMQConfig.SECKILL_VOUCHER_INVALIDATION_DLQ)
    public void onMessage(Message message, Map<String, Object> headers) {
        String body = new String(message.getBody());
        String key = (String) headers.get("spring_rabbitmq_routingKey");
        consumeRaw(body, key, headers);
    }

    @Override
    protected void doConsume(MessageExtend<SeckillVoucherInvalidationMessage> message) {
        SeckillVoucherInvalidationMessage body = message.getMessageBody();
        if (Objects.isNull(body.getVoucherId())) {
            log.warn("DLQ消息载荷为空或voucherId缺失, uuid={}", message.getUuid());
            safeInc("seckill_invalidation_dlq_replay_skipped", "reason", "invalid_payload");
            return;
        }

        safeInc("seckill_invalidation_dlq", "reason", "invalid_payload");

        auditLog.error("SECKILL_INVALIDATION_DLQ | message={}", JSON.toJSONString(message));
    }

    private void safeInc(String name, String tagKey, String tagValue) {
        try {
            if (meterRegistry != null) {
                meterRegistry.counter(name, tagKey, tagValue).increment();
            }
        } catch (Exception ignore) {
        }
    }
}
