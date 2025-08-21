package com.example.paymentservice.service;

import com.example.common.kafka.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * @author Anatoliy Shikin
 */
@Component
public class PaymentDltReplayer {
    private static final Logger log = LoggerFactory.getLogger(PaymentDltReplayer.class);
    private static final String RETRY_HDR = "x-retry-count";

    private final KafkaTemplate<String, String> template;
    private final int maxRetries;

    public PaymentDltReplayer(KafkaTemplate<String, String> template,
                              @Value("${dlt.replay.max-retries:3}") int maxRetries) {
        this.template = template;
        this.maxRetries = maxRetries;
    }

    // Автозапуск выключен - включить в properties dlt.replay.enabled=true
    @KafkaListener(topics = Topics.NEW_ORDERS_DLT,
            autoStartup = "${dlt.replay.enabled:false}",
            containerFactory = "kafkaListenerContainerFactory")
    public void replayNewOrders(ConsumerRecord<String, String> record, Acknowledgment ack) {
        int current = headerInt(record, RETRY_HDR, 0);
        if (current >= maxRetries) {
            log.warn("DLT drop (max retries reached) key={} p={} off={}", record.key(), record.partition(), record.offset());
            ack.acknowledge();
            return;
        }
        int next = current + 1;
        log.info("DLT replay key={} attempt={} -> {}", record.key(), next, Topics.NEW_ORDERS);
        template.send(Topics.NEW_ORDERS, record.key(), record.value());
        ack.acknowledge();
    }

    private int headerInt(ConsumerRecord<String, String> record, String name, int def) {
        Header header = record.headers().lastHeader(name);
        if (header == null) return def;
        try {
            return Integer.parseInt(new String(header.value(), StandardCharsets.UTF_8));
        } catch (Exception ignore) {
            return def;
        }
    }
}
