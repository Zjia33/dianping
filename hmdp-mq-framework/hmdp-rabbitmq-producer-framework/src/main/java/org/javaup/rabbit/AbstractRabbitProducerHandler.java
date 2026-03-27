package org.javaup.rabbit;


import com.alibaba.fastjson.JSON;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.javaup.message.MessageExtend;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;


/**
 * @description: RabbitMQ 消息发送抽象处理器。
 */
@Slf4j
@RequiredArgsConstructor
public abstract class AbstractRabbitProducerHandler<M extends MessageExtend<?>> {

    private final RabbitTemplate rabbitTemplate;

    public final CompletableFuture<Message> sendMqMessage(String exchange, M message) {
        Assert.hasText(exchange, "exchange must not be blank");
        Assert.notNull(message, "message must not be null");
        try {
            String routingKey = message.getKey() != null ? message.getKey() : "";
            CorrelationData correlationData = new CorrelationData(message.getUuid());

            CompletableFuture<Message> future = new CompletableFuture<>();

            rabbitTemplate.asyncConvertAndSend(exchange, routingKey, message, correlationData -> {
                if (correlationData.getFuture().isDoneExceptionally()) {
                    try {
                        correlationData.getFuture().get();
                    } catch (Exception e) {
                        future.completeExceptionally(e);
                        afterSendFailure(exchange, message, e);
                    }
                } else {
                    future.complete(null);
                    afterSendSuccess(exchange, message);
                }
                return null;
            });

            return future;
        } catch (Exception ex) {
            afterSendFailure(exchange, message, ex);
            CompletableFuture<Message> failed = new CompletableFuture<>();
            failed.completeExceptionally(ex);
            return failed;
        }
    }

    @SuppressWarnings("unchecked")
    public final <T> CompletableFuture<Message> sendPayload(String exchange, T payload) {
        M message = (M) MessageExtend.of(payload);
        return sendMqMessage(exchange, message);
    }

    @SuppressWarnings("unchecked")
    public final <T> CompletableFuture<Message> sendPayload(String exchange, String key, T payload, Map<String, String> headers) {
        M message = (M) MessageExtend.of(payload, key, headers);
        return sendMqMessage(exchange, message);
    }

    public final CompletableFuture<Message> sendRecord(String exchange, M message) {
        Assert.hasText(exchange, "exchange must not be blank");
        Assert.notNull(message, "message must not be null");

        String routingKey = message.getKey() != null ? message.getKey() : "";
        CorrelationData correlationData = new CorrelationData(message.getUuid());

        CompletableFuture<Message> future = new CompletableFuture<>();

        rabbitTemplate.asyncConvertAndSend(exchange, routingKey, message, correlationData1 -> {
            try {
                correlationData1.getFuture().get();
                future.complete(null);
                afterSendSuccess(exchange, message);
            } catch (Exception e) {
                future.completeExceptionally(e);
                afterSendFailure(exchange, message, e);
            }
            return null;
        });

        return future;
    }

    @SuppressWarnings("unchecked")
    public final <T> CompletableFuture<Void> sendBatch(String exchange, List<T> payloads) {
        Assert.hasText(exchange, "exchange must not be blank");
        Assert.notNull(payloads, "payloads must not be null");
        CompletableFuture<?>[] futures = payloads.stream()
                .map(p -> (M) MessageExtend.of(p))
                .map(m -> sendMqMessage(exchange, m))
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures);
    }

    public final <T> Message sendAndWait(String exchange, T payload) throws Exception {
        return sendPayload(exchange, payload).get();
    }

    @SuppressWarnings("unchecked")
    public final <T> CompletableFuture<Message> sendToDlq(String originalExchange, T payload, String reason) {
        String dlqExchange = originalExchange + ".DLX";
        M message = (M) MessageExtend.of(payload);
        message.setHeaders(Map.of("dlqReason", reason));
        return sendRecord(dlqExchange, message);
    }

    protected void afterSendSuccess(String exchange, M message) {
        log.info("rabbitmq message send success, exchange={}, routingKey={}, uuid={}",
                exchange, message.getKey(), message.getUuid());
    }

    protected void afterSendFailure(String exchange, M message, Throwable throwable) {
        log.error("rabbitmq message send failed, exchange={}, message={}", exchange, JSON.toJSON(message), throwable);
    }

    private static class Assert {
        public static void hasText(String text, String message) {
            if (text == null || text.trim().isEmpty()) {
                throw new IllegalArgumentException(message);
            }
        }

        public static void notNull(Object obj, String message) {
            if (obj == null) {
                throw new IllegalArgumentException(message);
            }
        }
    }
}
