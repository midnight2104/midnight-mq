package com.midnight.mq.server;

import lombok.AllArgsConstructor;
import lombok.Data;


@Data
@AllArgsConstructor
public class MessageSubscription {

    private String topic;
    private String consumerId;
    private int offset = -1;

}
