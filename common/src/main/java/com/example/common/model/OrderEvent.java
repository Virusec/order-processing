package com.example.common.model;

import java.time.Instant;

/**
 * @author Anatoliy Shikin
 */
public record OrderEvent(
        String orderId,
        String userId,
        String itemSku,
        int quantity,
        OrderStatus status,
        Instant ts
) {
}
