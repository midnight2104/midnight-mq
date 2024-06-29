package com.midnight.mq.core;

public interface MqListener<T> {
    void onMessage(MqMessage<T> message);
}
