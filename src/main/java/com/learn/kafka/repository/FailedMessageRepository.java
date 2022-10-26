package com.learn.kafka.repository;

import com.learn.kafka.entity.FailedMessage;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface FailedMessageRepository extends CrudRepository<FailedMessage, Integer> {
    List<FailedMessage> findAllByAction(String action);
}
