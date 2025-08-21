package com.example.shippingservice.service;

import com.example.common.kafka.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
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
public class ShippingDltReplayer {
    private static final Logger log = LoggerFactory.getLogger(ShippingDltReplayer.class);
    private static final String RETRY_HDR = "x-retry-count";

    private final KafkaTemplate<String, String> template;
    private final int maxRetries;

    public ShippingDltReplayer(KafkaTemplate<String, String> template,
                               @Value("${dlt.replay.max-retries:3}") int maxRetries) {
        this.template = template;
        this.maxRetries = maxRetries;
    }

    @KafkaListener(topics = Topics.PAYED_ORDERS_DLT,
            autoStartup = "${dlt.replay.enabled:false}",
            containerFactory = "kafkaListenerContainerFactory")
    public void replayPayed(ConsumerRecord<String, String> record, Acknowledgment ack) {
        int current = currentRetry(record);
        if (current >= maxRetries) {
            log.warn("DLT drop(shipping) key={} p={} off={}", record.key(), record.partition(), record.offset());
            ack.acknowledge();
            return;
        }
        int next = current + 1;
        log.info("DLT replay(shipping) key={} attempt={} -> {}", record.key(), next, Topics.PAYED_ORDERS);
        ProducerRecord<String, String> out = new ProducerRecord<>(Topics.PAYED_ORDERS, record.key(), record.value());
        out.headers().remove(RETRY_HDR);
        out.headers().add(new RecordHeader(RETRY_HDR, Integer.toString(next).getBytes(StandardCharsets.UTF_8)));
        template.send(out);
        ack.acknowledge();
    }

    private int currentRetry(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(RETRY_HDR);
        if (header == null) return 0;
        try { return Integer.parseInt(new String(header.value(), StandardCharsets.UTF_8)); }
        catch (Exception ignore) { return 0; }
    }
}
