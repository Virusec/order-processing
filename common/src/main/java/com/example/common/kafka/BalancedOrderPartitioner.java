package com.example.common.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * @author Anatoliy Shikin
 */
public class BalancedOrderPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if (numPartitions == 0) return 0;
        if (keyBytes == null && key != null) keyBytes = key.toString().getBytes(StandardCharsets.UTF_8);
        if (keyBytes == null || keyBytes.length == 0) {
            CRC32 crc = new CRC32();
            if (valueBytes != null) crc.update(valueBytes);
            return (int) (crc.getValue() % numPartitions);
        }
        CRC32 crc = new CRC32();
        crc.update(keyBytes);
        long hash = crc.getValue();
        return (int) (hash % numPartitions);
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
