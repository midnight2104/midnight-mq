package com.midnight.mq.core;

import com.midnight.mq.model.MqMessage;

public interface MqListener<T> {
    void onMessage(MqMessage<T> message);
}
