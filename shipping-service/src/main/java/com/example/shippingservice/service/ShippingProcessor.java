package com.example.shippingservice.service;

import com.example.common.kafka.Topics;
import com.example.common.model.OrderEvent;
import com.example.common.model.OrderStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Anatoliy Shikin
 */
@Service
public class ShippingProcessor {
    private static final Logger log = LoggerFactory.getLogger(ShippingProcessor.class);

    private final KafkaTemplate<String, String> template;
    private final ObjectMapper objectMapper;
    private final boolean autoCommit;
    private final boolean waitAcks;
    private final long sendTimeoutMs;

    public ShippingProcessor(KafkaTemplate<String, String> template,
                             ObjectMapper objectMapper,
                             @Value("${spring.kafka.consumer.enable-auto-commit:false}") boolean autoCommit,
                             @Value("${kafka.producer.wait-acks:false}") boolean waitAcks,
                             @Value("${kafka.producer.send-timeout-ms:5000}") long sendTimeoutMs) {
        this.template = template;
        this.objectMapper = objectMapper;
        this.autoCommit = autoCommit;
        this.waitAcks = waitAcks;
        this.sendTimeoutMs = sendTimeoutMs;
    }

    @KafkaListener(topics = Topics.PAYED_ORDERS, containerFactory = "kafkaListenerContainerFactory")
    public void onPayed(ConsumerRecord<String, String> record, Acknowledgment ack) throws Exception {
        log.debug("IN payed_orders p={} off={} key={}", record.partition(), record.offset(), record.key());

        final OrderEvent in;
        try {
            in = objectMapper.readValue(record.value(), OrderEvent.class);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Malformed OrderEvent json", exception);
        }

        if (in.status() != OrderStatus.PAYED) {
            if (!autoCommit && ack != null) ack.acknowledge();
            return;
        }
        Thread.sleep(10);

        OrderEvent out = new OrderEvent(in.orderId(), in.userId(), in.itemSku(), in.quantity(), OrderStatus.SENT, Instant.now());
        String payload = objectMapper.writeValueAsString(out);

        try {
            var future = template.send(Topics.SENT_ORDERS, out.orderId(), payload);
            if (waitAcks) {
                SendResult<String, String> result = future.get(sendTimeoutMs, TimeUnit.MILLISECONDS);
                log.debug("OUT sent_orders orderId={} -> {}-{}@{}",
                        out.orderId(), result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            } else {
                future.whenComplete((sendResult, throwable) -> {
                    if (throwable != null) {
                        log.error("FAIL sent_orders orderId={} cause={}", out.orderId(), throwable.toString());
                    } else {
                        log.debug("OUT sent_orders orderId={} -> {}-{}@{}",
                                out.orderId(), sendResult.getRecordMetadata().topic(),
                                sendResult.getRecordMetadata().partition(), sendResult.getRecordMetadata().offset());
                    }
                });
            }

            if (!autoCommit && ack != null) ack.acknowledge();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw exception;
        } catch (ExecutionException | TimeoutException exception) {
            log.error("Send to sent_orders failed for orderId={}, cause={}", out.orderId(), exception.toString());
            throw exception;
        }
    }
}
