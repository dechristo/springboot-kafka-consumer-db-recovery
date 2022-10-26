package com.learn.kafka.input;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learn.kafka.exception.LibraryEventNotFoundException;
import com.learn.kafka.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventsRetryConsumer
{
    @Autowired
    private LibraryEventService libraryEventService;

    @KafkaListener(topics = {"${topics.retry}"},
            autoStartup = "${retryListener.startup:true}",
            groupId = "retry-listener-group"
    )
    public void onMessage(ConsumerRecord<Integer,String> consumerRecord) throws JsonProcessingException, LibraryEventNotFoundException {
        log.info("ConsumerRecord in Retry Consumer : {} ", consumerRecord );
        consumerRecord.headers()
            .forEach(header -> {
                log.info("Key : {} , value : {}", header.key(), new String(header.value()));
            });
        libraryEventService.processEvent(consumerRecord);
    }
}
