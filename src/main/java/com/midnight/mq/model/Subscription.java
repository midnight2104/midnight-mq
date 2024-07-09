package com.midnight.mq.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Subscription {
    private String topic;
    private String consumerId;
    private int offset = -1;
}
