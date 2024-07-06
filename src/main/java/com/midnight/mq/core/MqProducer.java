package com.midnight.mq.core;

import com.midnight.mq.model.MqMessage;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class MqProducer {
    private MqBroker broker;

    public boolean send(String topic, MqMessage mqMessage) {
        return broker.send(topic, mqMessage);

    }
}
