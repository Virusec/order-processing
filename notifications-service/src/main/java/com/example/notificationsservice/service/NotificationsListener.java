package com.example.notificationsservice.service;

import com.example.common.kafka.Topics;
import com.example.common.model.OrderEvent;
import com.example.common.model.OrderStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * @author Anatoliy Shikin
 */
@Service
public class NotificationsListener {
    private final ObjectMapper objectMapper;

    public NotificationsListener(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = Topics.SENT_ORDERS, containerFactory = "kafkaListenerContainerFactory")
    public void onSent(ConsumerRecord<String, String> record) throws Exception {
        OrderEvent event = objectMapper.readValue(record.value(), OrderEvent.class);
        if (event.status() == OrderStatus.SENT) {
            System.out.printf("[NOTIFY] Заказ %s для пользователя %s успешно отправлен!%n", event.orderId(), event.userId());
        }
    }
}
