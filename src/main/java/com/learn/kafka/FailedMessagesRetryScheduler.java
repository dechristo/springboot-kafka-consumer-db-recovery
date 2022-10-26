package com.learn.kafka;

import com.learn.kafka.entity.FailedMessage;
import com.learn.kafka.enums.FailedMessageAction;
import com.learn.kafka.repository.FailedMessageRepository;
import com.learn.kafka.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class FailedMessagesRetryScheduler {

    @Autowired
    FailedMessageRepository failedMessageRepository;

    @Autowired
    LibraryEventService libraryEventService;

    @Scheduled(fixedDelay = 600000)
    public void retryFailedMessages() {
        log.info("Retry failed messages started.");

        failedMessageRepository.findAllByAction(FailedMessageAction.RETRY.toString())
            .forEach(failedMessage -> {
                log.debug("Retrying failed message: {}", failedMessage);
                var consumerRecord = buildFailedMessageConsumerRecord(failedMessage);
                try {
                    libraryEventService.processEvent(consumerRecord);
                    failedMessage.setAction(FailedMessageAction.DEAD_LETTER.toString());
                } catch (Exception ex) {
                    log.error("Error retrying failed messages: {}", ex.getMessage());
                }
            });

        log.info("Retry failed messages ended.");
    }

    private ConsumerRecord<Integer, String> buildFailedMessageConsumerRecord(FailedMessage failedMessage) {
        return new ConsumerRecord<>(
            failedMessage.getTopic(),
            failedMessage.getPartition(),
            failedMessage.getPartitionOffset(),
            failedMessage.getMessageKey(),
            failedMessage.getError()
        );
    }
}
