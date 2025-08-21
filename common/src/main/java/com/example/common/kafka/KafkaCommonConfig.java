package com.example.common.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Anatoliy Shikin
 */
@Configuration
public class KafkaCommonConfig {
    @Bean
    public NewTopic newOrders(@Value("${kafka.partitions:6}") int partitions) {
        return new NewTopic(Topics.NEW_ORDERS, partitions, (short)1);
    }
    @Bean
    public NewTopic payedOrders(@Value("${kafka.partitions:6}") int partitions) {
        return new NewTopic(Topics.PAYED_ORDERS, partitions, (short)1);
    }
    @Bean
    public NewTopic sentOrders(@Value("${kafka.partitions:6}") int partitions) {
        return new NewTopic(Topics.SENT_ORDERS, partitions, (short)1);
    }
    @Bean
    public NewTopic dltNew(@Value("${kafka.partitions:6}") int partitions) {
        return new NewTopic(Topics.NEW_ORDERS_DLT, partitions, (short)1);
    }
    @Bean
    public NewTopic dltPayed(@Value("${kafka.partitions:6}") int partitions) {
        return new NewTopic(Topics.PAYED_ORDERS_DLT, partitions, (short)1);
    }
    @Bean
    public NewTopic dltSent(@Value("${kafka.partitions:6}") int partitions) {
        return new NewTopic(Topics.SENT_ORDERS_DLT, partitions, (short)1);
    }
}
