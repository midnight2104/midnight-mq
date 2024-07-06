package com.midnight.mq.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class MqMessage<T> {

    private Long id;
    private T body;
    private Map<String, String> headers = new HashMap<>(); // 系统属性， X-version = 1.0

}
