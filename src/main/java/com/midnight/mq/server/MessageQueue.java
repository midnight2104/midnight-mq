package com.midnight.mq.server;

import com.midnight.mq.model.MqMessage;
import com.midnight.mq.model.Stat;
import com.midnight.mq.model.Subscription;
import com.midnight.mq.store.Indexer;
import com.midnight.mq.store.Store;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageQueue {
    public static final Map<String, MessageQueue> queues = new HashMap<>();
    public static final String TEST_TOPIC = "com.midnight.mq";

    static {
        queues.put(TEST_TOPIC, new MessageQueue(TEST_TOPIC));
        queues.put("demo", new MessageQueue("demo"));
    }

    private String topic;
    private Map<String, Subscription> subscriptions = new HashMap<>();

    @Getter
    private Store store;

    public MessageQueue(String topic) {
        this.topic = topic;
        this.store = new Store(topic);
        store.init();
    }

    public static List<MqMessage<?>> batch(String topic, String consumerId, int size) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");

        if (messageQueue.subscriptions.containsKey(consumerId)) {
            int offset = messageQueue.subscriptions.get(consumerId).getOffset();
            int nextOffset = 0;

            if (offset > -1) {
                Indexer.Entry entry = Indexer.getEntry(topic, offset);
                nextOffset = offset + entry.getLength();
            }

            List<MqMessage<?>> result = new ArrayList<>();
            MqMessage<?> recv = messageQueue.recv(nextOffset);
            while (recv != null) {
                result.add(recv);
                if (result.size() >= size) break;
                recv = messageQueue.recv(++offset);
            }
        }

        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }

    public static Stat stat(String topic, String consumerId) {
        MessageQueue queue = queues.get(topic);
        Subscription subscription = queue.subscriptions.get(consumerId);
        return new Stat(subscription, queue.getStore().total(), queue.getStore().pos());
    }

    public static void sub(Subscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        if (messageQueue == null) throw new RuntimeException("topic not found");
        messageQueue.subscribe(subscription);
    }


    public static void unsub(Subscription subscription) {
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
            int offset = messageQueue.subscriptions.get(consumerId).getOffset();
            int nextOffset = 0;
            if (offset > -1) {
                Indexer.Entry entry = Indexer.getEntry(topic, offset);
                nextOffset = offset + entry.getLength();
            }

            return messageQueue.recv(nextOffset);
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
            Subscription subscription = messageQueue.subscriptions.get(consumerId);
            // 只能往前消费
            if (offset > subscription.getOffset() && offset < Store.LEN) {
                subscription.setOffset(offset);
                return offset;
            }
            return -1;
        }

        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }


    private MqMessage<?> recv(int offset) {
        return store.read(offset);
    }

    private int send(MqMessage<String> message) {
        int offset = store.pos();
        message.getHeaders().put("X-offset", String.valueOf(offset));

        store.write(message);
        return offset;
    }

    private void unsubscribe(Subscription subscription) {
        String consumerId = subscription.getConsumerId();
        subscriptions.remove(consumerId);
    }

    private void subscribe(Subscription subscription) {
        String consumerId = subscription.getConsumerId();
        subscriptions.putIfAbsent(consumerId, subscription);
    }

}
