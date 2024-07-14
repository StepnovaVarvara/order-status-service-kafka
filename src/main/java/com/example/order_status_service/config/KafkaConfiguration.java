package com.example.order_status_service.config;

import com.example.order_status_service.dto.OrderEvent;
import com.example.order_status_service.dto.StatusEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.kafkaMessageGroupId}")
    private String kafkaMessageGroupId;

    // продуцирует сообщения
    @Bean
    public ProducerFactory<String, StatusEvent> kafkaMessageProducerFactory(ObjectMapper objectMapper) {
        Map<String, Object> config = new HashMap<>();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(config, new StringSerializer(), new JsonSerializer<>(objectMapper));
    }

    // отправитель сообщения
    @Bean
    public KafkaTemplate<String, StatusEvent> kafkaTemplate(ProducerFactory<String, StatusEvent> kafkaMessageProducerFactory) {
        return new KafkaTemplate<>(kafkaMessageProducerFactory);
    }

    // читает сообщения
    @Bean
    public ConsumerFactory<String, OrderEvent> kafkaMessageConsumerFactory(ObjectMapper objectMapper) {
        Map<String, Object> config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaMessageGroupId);
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), new JsonDeserializer<>(objectMapper));
    }

    // создает контейнеры, которые слушают Kafka
    // слушает Kafka и обрабатывает сообщения
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> kafkaMessageConcurrentKafkaListenerContainerFactory(
            ConsumerFactory<String, OrderEvent> kafkaMessageConsumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafkaMessageConsumerFactory);

        return factory;
    }
}
