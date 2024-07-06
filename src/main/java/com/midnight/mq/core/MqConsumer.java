package com.midnight.mq.core;

import com.midnight.mq.model.MqMessage;
import lombok.Getter;

import java.util.concurrent.atomic.AtomicInteger;

public class MqConsumer<T> {
    private MqBroker broker;
    private String id;
    static AtomicInteger idgen = new AtomicInteger(0);

    @Getter
    private MqListener listener;

    public MqConsumer(MqBroker broker) {
        this.broker = broker;
        this.id = "CID" + idgen.getAndIncrement();
    }

    public void sub(String topic) {
        broker.sub(topic, id);
    }

    public void unsub(String topic) {
        broker.unsub(topic, id);
    }

    public MqMessage<T> recv(String topic) {
        return broker.recv(topic, id);
    }


    public boolean ack(String topic, int offset) {
        return broker.ack(topic, id, offset);
    }

    public boolean ack(String topic, MqMessage<?> message) {
        int offset = Integer.parseInt(message.getHeaders().get("X-offset"));
        return ack(topic, offset);
    }

    public void listen(String topic, MqListener<T> listener) {
        this.listener = listener;
        broker.addConsumer(topic, this);
    }
}
