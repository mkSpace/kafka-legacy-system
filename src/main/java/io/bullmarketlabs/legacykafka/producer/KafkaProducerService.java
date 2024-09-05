package io.bullmarketlabs.legacykafka.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaProducerService {
    private final KafkaProducer<String, String> kafkaProducer;

    public void send(String topic, String message) {
        log.info("KafkaProducerService send message: {} to topic: {}", topic, message);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        kafkaProducer.send(record);
    }
}
