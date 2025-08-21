package com.example.shippingservice.service;

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
public class ShippingProcessor {
    private final KafkaTemplate<String, String> template;
    private final ObjectMapper objectMapper;
    private static final Logger log = LoggerFactory.getLogger(ShippingProcessor.class);

    public ShippingProcessor(KafkaTemplate<String, String> template, ObjectMapper objectMapper) {
        this.template = template;
        this.objectMapper = objectMapper;
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
            ack.acknowledge();
            return;
        }

        Thread.sleep(10);

        OrderEvent out = new OrderEvent(in.orderId(), in.userId(), in.itemSku(), in.quantity(), OrderStatus.SENT, Instant.now());
        String payload = objectMapper.writeValueAsString(out);

        template.send(Topics.SENT_ORDERS, out.orderId(), payload)
                .whenComplete((SendResult<String, String> res, Throwable throwable) -> {
                    if (throwable != null) {
                        log.error("FAIL sent_orders orderId={} cause={}", out.orderId(), throwable.toString());
                    } else {
                        log.debug("OUT sent_orders orderId={} -> {}-{}@{}",
                                out.orderId(),
                                res.getRecordMetadata().topic(),
                                res.getRecordMetadata().partition(),
                                res.getRecordMetadata().offset());
                    }
                });
        ack.acknowledge();
    }
}
