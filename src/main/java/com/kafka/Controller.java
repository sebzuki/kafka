package com.kafka;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequiredArgsConstructor
public class Controller {
    private final KafkaProperties kafkaProperties;

    @GetMapping("/test")
    public void produce() {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProperties.buildProducerProperties())) {
            String uuid = UUID.randomUUID().toString();
            final ProducerRecord<String, String> record = new ProducerRecord<>("TOPIC", uuid, uuid);
            producer.send(record, (metadata, e) -> {
                if (e != null)
                    System.out.println("Send failed for record {}" + record + e);
            });
        }
    }
}
