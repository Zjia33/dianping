package org.javaup.rabbitmq.producer;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.javaup.enums.SeckillVoucherOrderOperate;
import org.javaup.kafka.message.SeckillVoucherMessage;
import org.javaup.kafka.redis.RedisVoucherData;
import org.javaup.message.MessageExtend;
import org.javaup.rabbit.AbstractRabbitProducerHandler;
import org.javaup.rabbitmq.config.RabbitMQConfig;
import org.javaup.toolkit.SnowflakeIdGenerator;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import static org.javaup.constant.Constant.SECKILL_VOUCHER_TOPIC;
import static org.javaup.core.SpringUtil.getPrefixDistinctionName;

/**
 * @description: RabbitMQ 生产者：发送秒杀券
 */
@Slf4j
@Component
public class RabbitSeckillVoucherProducer extends AbstractRabbitProducerHandler<MessageExtend<SeckillVoucherMessage>> {

    @Resource
    private SnowflakeIdGenerator snowflakeIdGenerator;

    @Resource
    private RedisVoucherData redisVoucherData;

    public RabbitSeckillVoucherProducer(final RabbitTemplate rabbitTemplate) {
        super(rabbitTemplate);
    }

    public void sendSeckillVoucherMessage(SeckillVoucherMessage message) {
        String exchange = getPrefixDistinctionName() + "-" + SECKILL_VOUCHER_TOPIC;
        sendPayload(exchange, message);
    }

    @Override
    protected void afterSendFailure(final String exchange, final MessageExtend<SeckillVoucherMessage> message, final Throwable throwable) {
        super.afterSendFailure(exchange, message, throwable);
        long traceId = snowflakeIdGenerator.nextId();
        redisVoucherData.rollbackRedisVoucherData(
                SeckillVoucherOrderOperate.YES,
                traceId,
                message.getMessageBody().getVoucherId(),
                message.getMessageBody().getUserId(),
                message.getMessageBody().getOrderId(),
                message.getMessageBody().getAfterQty(),
                message.getMessageBody().getChangeQty(),
                message.getMessageBody().getBeforeQty());
    }
}
