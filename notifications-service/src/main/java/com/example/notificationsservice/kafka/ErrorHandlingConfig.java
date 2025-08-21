package com.example.notificationsservice.kafka;

import com.example.common.kafka.Topics;
import org.apache.kafka.common.TopicPartition;
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
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template, (record, exception) -> {
            String topic = record.topic();
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
        return handler;
    }
}
