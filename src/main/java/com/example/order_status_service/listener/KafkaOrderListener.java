package com.example.order_status_service.listener;

import com.example.order_status_service.dto.OrderEvent;
import com.example.order_status_service.dto.StatusEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@Slf4j
@RequiredArgsConstructor
public class KafkaOrderListener {

    @Value("${app.kafka.orderStatusTopic}")
    private String topicName;

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "${app.kafka.orderTopic}",
            groupId = "${app.kafka.kafkaMessageGroupId}")
    public void listen(ConsumerRecord<String, String> kafkaMessage) throws JsonProcessingException {

        objectMapper.registerModule(new JavaTimeModule());

        log.info("Received message: {}", kafkaMessage.value());
        log.info("Key: {}; Partition: {}; Topic: {}; Timestamp: {}", kafkaMessage.key(), kafkaMessage.partition(), kafkaMessage.topic(), kafkaMessage.timestamp());

        OrderEvent orderEvent = objectMapper.readValue(kafkaMessage.value(), OrderEvent.class);

        log.info("Order event from json = {}", orderEvent);

        StatusEvent statusEvent = new StatusEvent()
                .setStatus("CREATED")
                .setDate(Instant.now());

        String statusEventJsonToKafka = objectMapper.writeValueAsString(statusEvent);

        kafkaTemplate.send(topicName, statusEventJsonToKafka);
    }
}
