package com.midnight.mq.server;

import com.midnight.mq.model.MqMessage;
import com.midnight.mq.model.Result;
import com.midnight.mq.model.Stat;
import com.midnight.mq.model.Subscription;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;


@RestController
@RequestMapping("/mq")
public class MqServer {


    // send
    @RequestMapping("/send")
    public Result<String> send(@RequestParam("t") String topic,
                               @RequestBody MqMessage<String> message) {
        return Result.ok("" + MessageQueue.send(topic, message));
    }

    // recv
    @RequestMapping("/recv")
    public Result<MqMessage<?>> recv(@RequestParam("t") String topic,
                                     @RequestParam("cid") String consumerId) {
        return Result.msg(MessageQueue.recv(topic, consumerId));
    }

    // ack
    @RequestMapping("/ack")
    public Result<String> ack(@RequestParam("t") String topic,
                              @RequestParam("cid") String consumerId,
                              @RequestParam("offset") Integer offset) {
        return Result.ok("" + MessageQueue.ack(topic, consumerId, offset));
    }

    // 1.sub
    @RequestMapping("/sub")
    public Result<String> subscribe(@RequestParam("t") String topic,
                                    @RequestParam("cid") String consumerId) {
        MessageQueue.sub(new Subscription(topic, consumerId, -1));
        return Result.ok();
    }

    // unsub
    @RequestMapping("/unsub")
    public Result<String> unsubscribe(@RequestParam("t") String topic,
                                      @RequestParam("cid") String consumerId) {
        MessageQueue.unsub(new Subscription(topic, consumerId, -1));
        return Result.ok();
    }

    @RequestMapping("/stat")
    public Result<Stat> stat(@RequestParam("t") String topic,
                             @RequestParam("cid") String consumerId) {
        return Result.stat(MessageQueue.stat(topic, consumerId));
    }

    @RequestMapping("/batch")
    public Result<List<MqMessage<?>>> batch(@RequestParam("t") String topic,
                                          @RequestParam("cid") String consumerId,
                                          @RequestParam(name = "size", required = false, defaultValue = "1000") int size) {
        return Result.msg(MessageQueue.batch(topic, consumerId, size));
    }
}
