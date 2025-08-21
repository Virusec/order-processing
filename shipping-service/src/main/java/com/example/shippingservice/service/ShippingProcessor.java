package com.example.shippingservice.service;

import com.example.common.kafka.Topics;
import com.example.common.model.OrderEvent;
import com.example.common.model.OrderStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * @author Anatoliy Shikin
 */
@Service
public class ShippingProcessor {
    private final KafkaTemplate<String, String> template;
    private final ObjectMapper objectMapper;

    public ShippingProcessor(KafkaTemplate<String, String> template, ObjectMapper objectMapper) {
        this.template = template;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = Topics.PAYED_ORDERS, containerFactory = "kafkaListenerContainerFactory")
    public void onPayed(ConsumerRecord<String, String> record, Acknowledgment ack) throws Exception {
        OrderEvent in = objectMapper.readValue(record.value(), OrderEvent.class);
        if (in.status() != OrderStatus.PAYED) {
            ack.acknowledge();
            return;
        }

        // имитация отгрузки
        Thread.sleep(10);

        OrderEvent out = new OrderEvent(in.orderId(), in.userId(), in.itemSku(), in.quantity(), OrderStatus.SENT, Instant.now());
        template.send(Topics.SENT_ORDERS, out.orderId(), objectMapper.writeValueAsString(out));
        ack.acknowledge();
    }
}
