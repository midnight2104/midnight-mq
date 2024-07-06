package com.midnight.mq.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.midnight.mq.model.MqMessage;
import com.midnight.mq.model.Result;
import com.midnight.mq.utils.HttpUtils;
import com.midnight.mq.utils.ThreadUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

@Slf4j
public class MqBroker {
    @Getter
    public static MqBroker Default = new MqBroker();

    @Getter
    private MultiValueMap<String, MqConsumer<?>> consumers = new LinkedMultiValueMap<>();

    public static String brokerUrl = "http://localhost:8765/mq";

    static {
        init();
    }

    public static void init() {
        ThreadUtils.getDefault().init(1);
        ThreadUtils.getDefault().schedule(() -> {
            MultiValueMap<String, MqConsumer<?>> consumers = getDefault().getConsumers();
            consumers.forEach((topic, con) -> {
                con.forEach(consumer -> {
                    MqMessage<?> recv = consumer.recv(topic);
                    if (recv == null) return;

                    try {
                        consumer.getListener().onMessage(recv);
                        consumer.ack(topic, recv);
                    } catch (Exception ex) {
                        // todo
                    }
                });
            });
        }, 100, 100);
    }

    public MqProducer createProducer() {
        return new MqProducer(this);
    }

    public MqConsumer<?> createConsumer(String topic) {
        MqConsumer<?> consumer = new MqConsumer<>(this);
        consumer.sub(topic);
        return consumer;
    }

    public boolean send(String topic, MqMessage message) {
        log.info(" ==>> send topic/message: " + topic + "/" + message);
        Result<String> result = HttpUtils.httpPost(JSON.toJSONString(message),
                brokerUrl + "/send?t=" + topic, new TypeReference<Result<String>>() {
                });
        log.info(" ==>> send result: " + result);
        return result.getCode() == 1;
    }

    public void sub(String topic, String cid) {
        log.info(" ==>> sub topic/cid: " + topic + "/" + cid);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/sub?t=" + topic + "&cid=" + cid,
                new TypeReference<Result<String>>() {
                });
        log.info(" ==>> sub result: " + result);
    }

    public <T> MqMessage<T> recv(String topic, String id) {
        log.info(" ==>> recv topic/id: " + topic + "/" + id);
        Result<MqMessage<String>> result = HttpUtils.httpGet(
                brokerUrl + "/recv?t=" + topic + "&cid=" + id,
                new TypeReference<Result<MqMessage<String>>>() {
                });
        log.info(" ==>> recv result: " + result);
        return (MqMessage<T>) result.getData();
    }

    public void unsub(String topic, String cid) {
        log.info(" ==>> unsub topic/cid: " + topic + "/" + cid);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/unsub?t=" + topic + "&cid=" + cid,
                new TypeReference<Result<String>>() {
                });
        log.info(" ==>> unsub result: " + result);
    }

    public boolean ack(String topic, String cid, int offset) {
        log.info(" ==>> ack topic/cid/offset: " + topic + "/" + cid + "/" + offset);
        Result<String> result = HttpUtils.httpGet(
                brokerUrl + "/ack?t=" + topic + "&cid=" + cid + "&offset=" + offset,
                new TypeReference<Result<String>>() {
                });
        log.info(" ==>> ack result: " + result);
        return result.getCode() == 1;
    }

    public void addConsumer(String topic, MqConsumer<?> consumer) {
        consumers.add(topic, consumer);
    }

}

