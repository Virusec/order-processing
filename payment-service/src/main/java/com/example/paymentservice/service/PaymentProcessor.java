package com.example.paymentservice.service;

import com.example.common.kafka.Topics;
import com.example.common.model.OrderEvent;
import com.example.common.model.OrderStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * @author Anatoliy Shikin
 */
@Service
public class PaymentProcessor {
    private final KafkaTemplate<String, String> template;
    private final ObjectMapper objectMapper;
    private static final Logger log = LoggerFactory.getLogger(PaymentProcessor.class);

    public PaymentProcessor(KafkaTemplate<String, String> template, ObjectMapper objectMapper) {
        this.template = template;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = Topics.NEW_ORDERS, containerFactory = "kafkaListenerContainerFactory")
    public void onNewOrder(ConsumerRecord<String, String> record, Acknowledgment ack) throws Exception {
        String key = record.key();
        String value = record.value();
        log.debug("IN new_orders p={} off={} key={}", record.partition(), record.offset(), key);

        final OrderEvent in;
        try {
            in = objectMapper.readValue(value, OrderEvent.class);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Malformed OrderEvent json", exception);
        }
        if (in.status() != null && in.status() != OrderStatus.NEW) {
            ack.acknowledge();
            return;
        }

        Thread.sleep(10); // TODO: настроить на платёжный провайдер

        OrderEvent out = new OrderEvent(in.orderId(), in.userId(), in.itemSku(), in.quantity(), OrderStatus.PAYED, Instant.now());
        String payload = objectMapper.writeValueAsString(out);

        template.send(Topics.PAYED_ORDERS, out.orderId(), payload)
                .whenComplete((SendResult<String, String> result, Throwable throwable) -> {
                    if (throwable != null) {
                        log.error("FAIL payed_orders orderId={} cause={}", out.orderId(), throwable.toString());
                    } else {
                        log.debug("OUT payed_orders orderId={} -> {}-{}@{}",
                                out.orderId(),
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });

        ack.acknowledge();
    }
}
