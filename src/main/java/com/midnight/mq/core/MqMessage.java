package com.midnight.mq.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class MqMessage<T> {
    private Long id;
    private T body;
    private Map<String, String> headers;
}
