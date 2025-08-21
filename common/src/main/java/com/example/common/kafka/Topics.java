package com.example.common.kafka;

/**
 * @author Anatoliy Shikin
 */
public interface Topics {
    String NEW_ORDERS = "new_orders";
    String PAYED_ORDERS = "payed_orders";
    String SENT_ORDERS = "sent_orders";
    String NEW_ORDERS_DLT = "new_orders.DLT";
    String PAYED_ORDERS_DLT = "payed_orders.DLT";
    String SENT_ORDERS_DLT = "sent_orders.DLT";
}
