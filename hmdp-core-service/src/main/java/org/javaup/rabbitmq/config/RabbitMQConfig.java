package org.javaup.rabbitmq.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @description: RabbitMQ 配置类
 */
@Configuration
public class RabbitMQConfig {

    public static final String SECKILL_VOUCHER_EXCHANGE = "seckill_voucher_exchange";
    public static final String SECKILL_VOUCHER_QUEUE = "seckill_voucher_queue";
    public static final String SECKILL_VOUCHER_ROUTING_KEY = "seckill_voucher_topic";
    public static final String SECKILL_VOUCHER_DLX = "seckill_voucher_dlx";
    public static final String SECKILL_VOUCHER_DLQ = "seckill_voucher_dlq";
    public static final String SECKILL_VOUCHER_DLQ_ROUTING_KEY = "seckill_voucher_topic.DLQ";

    public static final String SECKILL_VOUCHER_INVALIDATION_EXCHANGE = "seckill_voucher_invalidation_exchange";
    public static final String SECKILL_VOUCHER_INVALIDATION_QUEUE = "seckill_voucher_invalidation_queue";
    public static final String SECKILL_VOUCHER_INVALIDATION_ROUTING_KEY = "seckill_voucher_cache_invalidation_topic";
    public static final String SECKILL_VOUCHER_INVALIDATION_DLX = "seckill_voucher_invalidation_dlx";
    public static final String SECKILL_VOUCHER_INVALIDATION_DLQ = "seckill_voucher_invalidation_dlq";
    public static final String SECKILL_VOUCHER_INVALIDATION_DLQ_ROUTING_KEY = "seckill_voucher_cache_invalidation_topic.DLQ";

    @Bean
    public MessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(messageConverter());
        return template;
    }

    // ==================== Seckill Voucher Exchange & Queue ====================

    @Bean
    public TopicExchange seckillVoucherExchange() {
        return new TopicExchange(SECKILL_VOUCHER_EXCHANGE, true, false);
    }

    @Bean
    public Queue seckillVoucherQueue() {
        return QueueBuilder.durable(SECKILL_VOUCHER_QUEUE)
                .withArgument("x-dead-letter-exchange", SECKILL_VOUCHER_DLX)
                .withArgument("x-dead-letter-routing-key", SECKILL_VOUCHER_DLQ_ROUTING_KEY)
                .build();
    }

    @Bean
    public Binding seckillVoucherBinding() {
        return BindingBuilder.bind(seckillVoucherQueue())
                .to(seckillVoucherExchange())
                .with(SECKILL_VOUCHER_ROUTING_KEY);
    }

    // Seckill Voucher DLX & DLQ

    @Bean
    public TopicExchange seckillVoucherDlqExchange() {
        return new TopicExchange(SECKILL_VOUCHER_DLX, true, false);
    }

    @Bean
    public Queue seckillVoucherDlqQueue() {
        return QueueBuilder.durable(SECKILL_VOUCHER_DLQ).build();
    }

    @Bean
    public Binding seckillVoucherDlqBinding() {
        return BindingBuilder.bind(seckillVoucherDlqQueue())
                .to(seckillVoucherDlqExchange())
                .with(SECKILL_VOUCHER_DLQ_ROUTING_KEY);
    }

    // ==================== Seckill Voucher Invalidation Exchange & Queue ====================

    @Bean
    public TopicExchange seckillVoucherInvalidationExchange() {
        return new TopicExchange(SECKILL_VOUCHER_INVALIDATION_EXCHANGE, true, false);
    }

    @Bean
    public Queue seckillVoucherInvalidationQueue() {
        return QueueBuilder.durable(SECKILL_VOUCHER_INVALIDATION_QUEUE)
                .withArgument("x-dead-letter-exchange", SECKILL_VOUCHER_INVALIDATION_DLX)
                .withArgument("x-dead-letter-routing-key", SECKILL_VOUCHER_INVALIDATION_DLQ_ROUTING_KEY)
                .build();
    }

    @Bean
    public Binding seckillVoucherInvalidationBinding() {
        return BindingBuilder.bind(seckillVoucherInvalidationQueue())
                .to(seckillVoucherInvalidationExchange())
                .with(SECKILL_VOUCHER_INVALIDATION_ROUTING_KEY);
    }

    // Seckill Voucher Invalidation DLX & DLQ

    @Bean
    public TopicExchange seckillVoucherInvalidationDlqExchange() {
        return new TopicExchange(SECKILL_VOUCHER_INVALIDATION_DLX, true, false);
    }

    @Bean
    public Queue seckillVoucherInvalidationDlqQueue() {
        return QueueBuilder.durable(SECKILL_VOUCHER_INVALIDATION_DLQ).build();
    }

    @Bean
    public Binding seckillVoucherInvalidationDlqBinding() {
        return BindingBuilder.bind(seckillVoucherInvalidationDlqQueue())
                .to(seckillVoucherInvalidationDlqExchange())
                .with(SECKILL_VOUCHER_INVALIDATION_DLQ_ROUTING_KEY);
    }
}
