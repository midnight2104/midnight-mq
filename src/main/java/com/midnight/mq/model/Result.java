package com.midnight.mq.model;

import lombok.AllArgsConstructor;
import lombok.Data;


@AllArgsConstructor
@Data
public class Result<T> {
    private int code; // 1==success, 0==fail
    private T data;

    public static Result<String> ok() {
        return new Result<>(1, "OK");
    }

    public static Result<String> ok(String msg) {
        return new Result<>(1, msg);
    }


    public static Result<MqMessage<?>> msg(MqMessage<?> msg) {
        return new Result<>(1, msg);
    }
}
