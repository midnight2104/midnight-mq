package com.midnight.mq.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class MqMessage<T> {
    private static AtomicLong idgen = new AtomicLong(0);

    private Long id;
    private T body;
    private Map<String, String> headers = new HashMap<>(); // 系统属性， X-version = 1.0


    public static long nextId() {
        return idgen.getAndIncrement();
    }

    public static MqMessage<String> create(String body, Map<String, String> headers) {
        return new MqMessage<>(nextId(), body, headers);
    }
}
