package com.example.kafka.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchAcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Service
public class KafkaConsumerService implements BatchAcknowledgingConsumerAwareMessageListener<String, String> {

    @KafkaListener(
            topics = "clickHouse-topic",
            groupId = "json-group",
            containerFactory = "kafkaListenerStringContainerAckFactory",
            autoStartup = "true")
    public void onMessage(List<ConsumerRecord<String, String>> data, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {
        if (log.isDebugEnabled()) log.debug("ADS data size : {}", data.size());
        for (ConsumerRecord<String, String> record : data) {
            log.info("✅ Received Kafka Message: {}", record.value());
        }
        acknowledgment.acknowledge();
    }

}