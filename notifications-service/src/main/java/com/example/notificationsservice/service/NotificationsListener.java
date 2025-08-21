package com.example.notificationsservice.service;

import com.example.common.kafka.Topics;
import com.example.common.model.OrderEvent;
import com.example.common.model.OrderStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * @author Anatoliy Shikin
 */
@Service
public class NotificationsListener {
    private final ObjectMapper objectMapper;
    private static final Logger log = LoggerFactory.getLogger(NotificationsListener.class);

    public NotificationsListener(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = Topics.SENT_ORDERS, containerFactory = "kafkaListenerContainerFactory")
    public void onSent(ConsumerRecord<String, String> record) {
        final OrderEvent event;
        try {
            event = objectMapper.readValue(record.value(), OrderEvent.class);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Malformed OrderEvent json", exception);
        }
        if (event.status() == OrderStatus.SENT) {
            log.info("[NOTIFY] orderId={} userId={} delivered", event.orderId(), event.userId());
        } else {
            log.debug("Skip notify orderId={} status={}", event.orderId(), event.status());
        }
    }
}
