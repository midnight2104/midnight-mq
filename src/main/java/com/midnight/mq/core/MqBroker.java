package com.midnight.mq.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MqBroker {
    Map<String, MidnightMq> mq = new ConcurrentHashMap<>();

    public MidnightMq find(String topic) {
        return mq.get(topic);
    }

    public MidnightMq createTopic(String topic) {
        return mq.putIfAbsent(topic, new MidnightMq(topic));
    }

    public MqProducer createProducer() {
        return new MqProducer(this);
    }

    public MqConsumer createConsumer(String topic) {
        MqConsumer consumer = new MqConsumer(this);
        consumer.subscribe(topic);
        return consumer;
    }
}
