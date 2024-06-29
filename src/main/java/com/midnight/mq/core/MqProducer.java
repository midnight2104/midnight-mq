package com.midnight.mq.core;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class MqProducer {
    private MqBroker broker;

    public boolean send(String topic, MqMessage mqMessage) {
        MidnightMq mq = broker.find(topic);
        if (mq == null) {
            throw new RuntimeException("topic not find");
        }
        return mq.send(mqMessage);
    }
}
