package com.example.shippingservice.kafka;

import com.example.common.kafka.Topics;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * @author Anatoliy Shikin
 */
@Configuration
public class ErrorHandlingConfig {
    private static final Logger log = LoggerFactory.getLogger(ErrorHandlingConfig.class);

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template, (record, exception) -> {
            String topic = record.topic();
            log.error("DLT publish (shipping): topic={} p={} off={} key={} cause={}",
                    record.topic(), record.partition(), record.offset(), record.key(), exception.toString());
            return switch (topic) {
                case Topics.NEW_ORDERS -> new TopicPartition(Topics.NEW_ORDERS_DLT, record.partition());
                case Topics.PAYED_ORDERS -> new TopicPartition(Topics.PAYED_ORDERS_DLT, record.partition());
                case Topics.SENT_ORDERS -> new TopicPartition(Topics.SENT_ORDERS_DLT, record.partition());
                default -> new TopicPartition(topic + ".DLT", record.partition());
            };
        });
        ExponentialBackOff backOff = new ExponentialBackOff(200L, 2.0);
        backOff.setMaxElapsedTime(5_000L);
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.setAckAfterHandle(true);
        handler.setCommitRecovered(true);
        handler.addNotRetryableExceptions(IllegalArgumentException.class, JsonProcessingException.class);
        handler.setRetryListeners((record, exception, attempt) ->
                log.warn("Retry(shipping) {}/? topic={} p={} off={} key={} reason={}",
                        attempt, record.topic(), record.partition(), record.offset(), record.key(), exception.toString()));
        return handler;
    }
}
