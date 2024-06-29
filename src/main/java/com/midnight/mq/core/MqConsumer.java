package com.midnight.mq.core;

public class MqConsumer {
    private MqBroker broker;
    private String topic;
    MidnightMq mq;

    public MqConsumer(MqBroker broker) {
        this.broker = broker;
    }

    public void subscribe(String topic) {
        this.topic = topic;
        this.mq = broker.find(topic);
        if (mq == null) throw new RuntimeException("topic not found");

    }

    public <T> MqMessage<T> poll(long timeout) {
        return mq.poll(timeout);
    }

    public <T> void listen(MqListener<T> listener) {
        mq.addListener(listener);
    }
}
