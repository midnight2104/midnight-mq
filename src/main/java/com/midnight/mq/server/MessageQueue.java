package com.midnight.mq.server;

import com.midnight.mq.model.MqMessage;

import java.util.HashMap;
import java.util.Map;

public class MessageQueue {
    public static final Map<String, MessageQueue> queues = new HashMap<>();
    public static final String TEST_TOPIC = "com.midnight.mq";

    static {
        queues.put(TEST_TOPIC, new MessageQueue(TEST_TOPIC));
        queues.put("demo", new MessageQueue("demo"));
    }

    private String topic;
    private int index = 0;
    private MqMessage<?>[] queue = new MqMessage[1024 * 10];
    private Map<String, MessageSubscription> subscriptions = new HashMap<>();

    public MessageQueue(String topic) {
        this.topic = topic;
    }

    public static void sub(MessageSubscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        if (messageQueue == null) throw new RuntimeException("topic not found");
        messageQueue.subscribe(subscription);
    }


    public static void unsub(MessageSubscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        if (messageQueue == null) return;
        messageQueue.unsubscribe(subscription);
    }

    public static int send(String topic, MqMessage<String> message) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        return messageQueue.send(message);
    }

    // 使用此方法，需要手工调用ack，更新订阅关系里的offset
    public static MqMessage<?> recv(String topic, String consumerId) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");

        if (messageQueue.subscriptions.containsKey(consumerId)) {
            int ind = messageQueue.subscriptions.get(consumerId).getOffset();
            return messageQueue.recv(ind + 1);
        }

        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }

    public static MqMessage<?> recv(String topic, String consumerId, int ind) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            return messageQueue.recv(ind);
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }

    public static int ack(String topic, String consumerId, int offset) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            MessageSubscription subscription = messageQueue.subscriptions.get(consumerId);
            // 只能往前消费
            if (offset > subscription.getOffset() && offset < messageQueue.index) {
                subscription.setOffset(offset);
                return offset;
            }
            return -1;
        }

        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }


    private MqMessage<?> recv(int ind) {
        if (ind < index) {
            return queue[ind];
        }
        return null;
    }

    private int send(MqMessage<String> message) {
        if (index > queue.length) {
            return -1;
        }
        message.getHeaders().put("X-offset", String.valueOf(index));
        queue[index++] = message;
        return index;
    }

    private void unsubscribe(MessageSubscription subscription) {
        String consumerId = subscription.getConsumerId();
        subscriptions.remove(consumerId);
    }

    private void subscribe(MessageSubscription subscription) {
        String consumerId = subscription.getConsumerId();
        subscriptions.putIfAbsent(consumerId, subscription);
    }

}
