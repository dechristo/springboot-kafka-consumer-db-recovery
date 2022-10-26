package com.learn.kafka.config;

import com.learn.kafka.enums.FailedMessageAction;
import com.learn.kafka.exception.LibraryEventNotFoundException;
import com.learn.kafka.service.FailedMessageService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.InvalidPropertyException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;
import java.util.List;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    private static final String RETRY = "RETRY";
    private static final String DEADLETTER = "DEADLETTER";

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    FailedMessageService failedMessageService;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dead-letter}")
    private String deadLetterTopic;

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
        ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
        ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(consumerErrorHandler());
        return factory;
    }

    ConsumerRecordRecoverer consumerRecordRecoverer = (consumerRecord, e) -> {
        log.error("Error consuming message: {}", e.getMessage());

        if (e.getCause() instanceof RecoverableDataAccessException) {
            failedMessageService.save((ConsumerRecord<Integer, String>) consumerRecord, e, FailedMessageAction.RETRY);
        } else {
            failedMessageService.save((ConsumerRecord<Integer, String>) consumerRecord, e, FailedMessageAction.DEAD_LETTER);
        }
    };

    public DefaultErrorHandler consumerErrorHandler() {
        var retryableExceptions = List.of(RecoverableDataAccessException.class);
        var exceptionsToIgnore = List.of(
            IllegalArgumentException.class,
            LibraryEventNotFoundException.class
        );
        var fixedBackOff = new FixedBackOff(1000L, 2);
        var exponentialBackOff = new ExponentialBackOffWithMaxRetries(2);
        exponentialBackOff.setInitialInterval(1000L);
        exponentialBackOff.setMultiplier(2);
        exponentialBackOff.setMaxInterval(2000L);

        var errorHandler = new DefaultErrorHandler(consumerRecordRecoverer, fixedBackOff);

        exceptionsToIgnore.forEach(errorHandler::addNotRetryableExceptions);

        retryableExceptions.forEach(errorHandler::addRetryableExceptions);

        errorHandler.setRetryListeners((((record, ex, deliveryAttempt) -> {
            log.error("Failed Record in Retry Listener, Exception: {}", ex.getMessage());
            log.error("Failed Record in Retry Listener, Delivery Attempt: {}", deliveryAttempt);
        })));

        return errorHandler;
    }
}
