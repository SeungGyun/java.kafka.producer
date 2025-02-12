package com.example.kafka.service;

import com.example.kafka.dto.KafkaMessageDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaProducerService {
    private static final String TOPIC = "clickHouse-topic";
    private final KafkaTemplate<String, KafkaMessageDto> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, KafkaMessageDto> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(KafkaMessageDto message) {
        kafkaTemplate.send(TOPIC, message)
                .whenComplete((result, exception) -> {
                    if (exception != null) {
                        log.error("❌ Send Fail - Topic: {}, Error: {}", TOPIC, exception.getMessage(), exception);
                    } else {
                        log.info("✅ Send Success - Topic: {}, Partition: {}", result.getRecordMetadata().topic(), result.getRecordMetadata().partition());
                    }
                });
    }
}
