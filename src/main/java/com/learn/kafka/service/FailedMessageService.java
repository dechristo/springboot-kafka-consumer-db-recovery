package com.learn.kafka.service;

import com.learn.kafka.entity.FailedMessage;
import com.learn.kafka.enums.FailedMessageAction;
import com.learn.kafka.repository.FailedMessageRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class FailedMessageService {

    @Autowired
    FailedMessageRepository failedMessageRepository;

    public void save(ConsumerRecord<Integer, String> consumerRecord, Exception ex, FailedMessageAction action) {
        FailedMessage newFailedMessage = FailedMessage.builder()
            .id(null)
            .topic(consumerRecord.topic())
            .messageKey(consumerRecord.key())
            .message(consumerRecord.value())
            .partition(consumerRecord.partition())
            .partitionOffset(consumerRecord.offset())
            .error(ex.getCause().getMessage())
            .action(action.toString())
            .build();

        failedMessageRepository.save(newFailedMessage);
    }
}
