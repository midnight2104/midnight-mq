package com.midnight.mq.demo;

import com.alibaba.fastjson.JSON;
import com.midnight.mq.core.MqBroker;
import com.midnight.mq.core.MqConsumer;
import com.midnight.mq.core.MqProducer;
import com.midnight.mq.model.MqMessage;
import lombok.SneakyThrows;

/**
 * mq demo for order.
 *
 * @Author : kimmking(kimmking@apache.org)
 * @create 2024/6/25 下午9:10
 */
public class MqDemo {


    @SneakyThrows
    public static void main(String[] args) {

        long ids = 0;

        String topic = "com.midnight.mq";
        MqBroker broker = MqBroker.getDefault();

        MqProducer producer = broker.createProducer();
//        MqConsumer<?> consumer = broker.createConsumer(topic);
//        consumer.listen(topic, message -> {
//            System.out.println(" onMessage => " + message); // 这里处理消息
//        });

        MqConsumer<?> consumer1 = broker.createConsumer(topic);

        for (int i = 0; i < 2; i++) {
            Order order = new Order(ids, "item" + ids, 100 * ids);
            producer.send(topic, new MqMessage<>((long) ids++, JSON.toJSONString(order), null));
        }

        for (int i = 0; i < 2; i++) {
            MqMessage<String> message = (MqMessage<String>) consumer1.recv(topic);
            System.out.println("业务处理===>>>"+message);
            consumer1.ack(topic, message);
        }

        while (true) {
            char c = (char) System.in.read();
            if (c == 'q' || c == 'e') {
                 consumer1.unsub(topic);
                break;
            }
            if (c == 'p') {
                Order order = new Order(ids, "item" + ids, 100 * ids);
                producer.send(topic, new MqMessage<>(ids++, JSON.toJSONString(order), null));
                System.out.println("produce ok => " + order);
            }
            if (c == 'c') {
                MqMessage<String> message = (MqMessage<String>) consumer1.recv(topic);
                System.out.println("consume ok => " + message);
                consumer1.ack(topic, message);
            }
            if (c == 'a') {
                for (int i = 0; i < 10; i++) {
                    Order order = new Order(ids, "item" + ids, 100 * ids);
                    producer.send(topic, new MqMessage<>((long) ids++, JSON.toJSONString(order), null));
                }
                System.out.println("produce 10 orders...");
            }
        }

    }

}
