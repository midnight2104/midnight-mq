package com.midnight.mq.core;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@AllArgsConstructor
public class MidnightMq {
    private String topic;
    private LinkedBlockingQueue<MqMessage> queue = new LinkedBlockingQueue<>();
    private List<MqListener> listeners = new ArrayList<>();

    public MidnightMq(String topic) {
        this.topic = topic;
    }

    public boolean send(MqMessage msg) {
        boolean offer = queue.offer(msg);
        listeners.forEach(mqListener -> mqListener.onMessage(msg));
        return offer;
    }

    @SneakyThrows
    public <T> MqMessage<T> poll(long timeout) {
        return queue.poll(timeout, TimeUnit.MILLISECONDS);
    }

    public <T> void addListener(MqListener<T> listener) {
        listeners.add(listener);
    }
}
