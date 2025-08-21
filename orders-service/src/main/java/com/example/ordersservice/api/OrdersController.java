package com.example.ordersservice.api;

import com.example.common.kafka.Topics;
import com.example.common.model.OrderEvent;
import com.example.common.model.OrderStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

/**
 * @author Anatoliy Shikin
 */
@RestController
@RequestMapping("/orders")
public class OrdersController {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public OrdersController(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @PostMapping
    public ResponseEntity<?> create(@RequestBody OrderRequest request) throws JsonProcessingException {
        OrderEvent orderEvent = new OrderEvent(request.orderId(), request.userId(), request.itemSku(), request.quantity(), OrderStatus.NEW, Instant.now());
        String payload = objectMapper.writeValueAsString(orderEvent);
        kafkaTemplate.send(Topics.NEW_ORDERS, orderEvent.orderId(), payload);
        return ResponseEntity.accepted().body(orderEvent);
    }

    @PatchMapping("/{orderId}/status/{status}")
    public ResponseEntity<?> update(@PathVariable String orderId, @PathVariable String status) {
        try {
            OrderEvent orderEvent = new OrderEvent(orderId, null, null, 0, OrderStatus.valueOf(status), Instant.now());
            kafkaTemplate.send(Topics.NEW_ORDERS, orderId, objectMapper.writeValueAsString(orderEvent));
            return ResponseEntity.ok(orderEvent);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }
}
