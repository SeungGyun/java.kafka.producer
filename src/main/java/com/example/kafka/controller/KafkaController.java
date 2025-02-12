package com.example.kafka.controller;

import com.example.kafka.dto.KafkaMessageDto;
import com.example.kafka.service.KafkaProducerService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {
    private final KafkaProducerService kafkaProducerService;

    public KafkaController(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @PostMapping("/send")
    public String sendMessage(@RequestBody KafkaMessageDto message) {
        kafkaProducerService.sendMessage(message);
        return "Message sent: " + message;
    }
}
