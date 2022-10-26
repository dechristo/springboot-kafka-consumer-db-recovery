package com.learn.kafka.entity;

import com.learn.kafka.enums.FailedMessageAction;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
public class FailedMessage {

    @Id
    @GeneratedValue
    private Integer id;
    private String topic;
    private Integer messageKey;
    private String message;
    private Integer partition;
    private Long partitionOffset;
    private String error;
    private String action;
}
