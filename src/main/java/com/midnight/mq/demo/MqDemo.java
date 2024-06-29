package com.midnight.mq.demo;

import com.midnight.mq.core.MqBroker;
import com.midnight.mq.core.MqConsumer;
import com.midnight.mq.core.MqMessage;
import com.midnight.mq.core.MqProducer;
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

        String topic = "midnight.order";
        MqBroker broker = new MqBroker();
        broker.createTopic(topic);

        MqProducer producer = broker.createProducer();
        MqConsumer consumer = broker.createConsumer(topic);
        consumer.listen(message -> {
            System.out.println(" onMessage => " + message);
        });


        for (int i = 0; i < 10; i++) {
            Order order = new Order(ids, "item" + ids, 100 * ids);
            producer.send(topic, new MqMessage((long) ids++, order, null));
        }

        for (int i = 0; i < 10; i++) {
            MqMessage<Order> message = consumer.poll(1000);
            System.out.println(message);
        }

        while (true) {
            char c = (char) System.in.read();
            if (c == 'q' || c == 'e') {
                break;
            }
            if (c == 'p') {
                Order order = new Order(ids, "item" + ids, 100 * ids);
                producer.send(topic, new MqMessage<>(ids++, order, null));
                System.out.println("send ok => " + order);
            }
            if (c == 'c') {
                MqMessage<Order> message = consumer.poll(1000);
                System.out.println("poll ok => " + message);
            }
            if (c == 'a') {
                for (int i = 0; i < 10; i++) {
                    Order order = new Order(ids, "item" + ids, 100 * ids);
                    producer.send(topic, new MqMessage<>((long) ids++, order, null));
                }
                System.out.println("send 10 orders...");
            }
        }

    }

}
