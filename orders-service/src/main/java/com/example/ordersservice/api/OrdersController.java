package com.example.ordersservice.api;

import com.example.common.kafka.Topics;
import com.example.common.model.OrderEvent;
import com.example.common.model.OrderStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
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
    private static final Logger log = LoggerFactory.getLogger(OrdersController.class);

    public OrdersController(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @PostMapping
    public ResponseEntity<?> create(@RequestBody OrderRequest request) throws JsonProcessingException {
        OrderEvent orderEvent = new OrderEvent(request.orderId(), request.userId(), request.itemSku(), request.quantity(), OrderStatus.NEW, Instant.now());
        String payload = objectMapper.writeValueAsString(orderEvent);
        log.info("CREATE orderId={} userId={} sku={} qty={}", orderEvent.orderId(), orderEvent.userId(), orderEvent.itemSku(), orderEvent.quantity());
        kafkaTemplate.send(Topics.NEW_ORDERS, orderEvent.orderId(), payload)
                .whenComplete((SendResult<String, String> result, Throwable throwable) -> {
                    if (throwable != null) {
                        log.error("Failed to send NEW_ORDERS orderId={}", orderEvent.orderId(), throwable);
                    } else {
                        log.debug("Sent NEW_ORDERS orderId={} to {}-{}@{}",
                                orderEvent.orderId(),
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
        return ResponseEntity.accepted().body(orderEvent);
    }

    @PatchMapping("/{orderId}/status/{status}")
    public ResponseEntity<?> update(@PathVariable String orderId, @PathVariable String status) {
        try {
            OrderEvent orderEvent = new OrderEvent(orderId, null, null, 0, OrderStatus.valueOf(status), Instant.now());
            String payload = objectMapper.writeValueAsString(orderEvent);
            log.info("UPDATE orderId={} -> {}", orderId, status);
            kafkaTemplate.send(Topics.NEW_ORDERS, orderId, payload);
            return ResponseEntity.ok(orderEvent);
        } catch (Exception exception) {
            log.warn("Bad status PATCH orderId={} status={} : {}", orderId, status, exception.toString());
            return ResponseEntity.badRequest().body(exception.getMessage());
        }
    }
}
