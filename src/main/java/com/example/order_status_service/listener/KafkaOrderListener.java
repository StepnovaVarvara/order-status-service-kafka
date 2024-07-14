package com.example.order_status_service.listener;

import com.example.order_status_service.dto.OrderEvent;
import com.example.order_status_service.dto.StatusEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Component
@Slf4j
@RequiredArgsConstructor
public class KafkaOrderListener {

    @Value("${app.kafka.orderStatusTopic}")
    private String topicName;

    private final KafkaTemplate<String, StatusEvent> kafkaTemplate;

    // обрабатывает сообщения из Kafka
    @KafkaListener(topics = "${app.kafka.orderTopic}",
            groupId = "${app.kafka.kafkaMessageGroupId}",
            containerFactory = "kafkaMessageConcurrentKafkaListenerContainerFactory")
    public void listen(@Payload OrderEvent orderEvent,
                       @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) UUID key,
                       @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
                       @Header(KafkaHeaders.RECEIVED_TIMESTAMP) Long timestamp) {

        log.info("Received message: {}", orderEvent);
        log.info("Key: {}; Partition: {}; Topic: {}; Timestamp: {}", key, partition, topic, timestamp);

        StatusEvent statusEvent = new StatusEvent()
                .setStatus("CREATED")
                .setDate(Instant.now());

        kafkaTemplate.send(topicName, statusEvent);
    }
}
