package com.example.ordersservice.api;

/**
 * @author Anatoliy Shikin
 */
public record OrderRequest(
        String orderId,
        String userId,
        String itemSku,
        int quantity
) {
}
