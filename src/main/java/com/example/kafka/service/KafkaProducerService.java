package com.example.kafka.service;

import com.example.kafka.config.KafkaConfig;
import com.example.kafka.dto.KafkaMessageDto;
import com.example.kafka.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaProducerService {
    private static final String TOPIC = "clickHouse-topic";
    private final KafkaProducer<String, String> kafkaProducer;

    public KafkaProducerService(KafkaConfig kafkaConfig) {
        this.kafkaProducer = new KafkaProducer<String, String>(kafkaConfig.producerConfig());
    }

    public void sendMessage(KafkaMessageDto message) {
        kafkaProducer.send(new ProducerRecord<String, String>(TOPIC, JsonUtil.writeValueAsString(message)), new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e != null) {
                    log.error("Send Fail - SendTopic : {}, ErrMessage : {}", metadata.topic(), e.getMessage(), e);
                } else {
                    if (log.isDebugEnabled())
                        log.debug("Send Success - SendTopic: {}, Partition: {}", metadata.topic(), metadata.partition());
                }
            }
        });
    }

}
