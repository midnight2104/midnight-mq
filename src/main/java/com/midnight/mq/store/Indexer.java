package com.midnight.mq.store;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Indexer {
    private static MultiValueMap<String, Entry> indexes = new LinkedMultiValueMap<>();
    private static Map<String, Map<Integer, Entry>> mappings = new HashMap<>();

    @AllArgsConstructor
    @Data
    public static class Entry {
        int offset;
        int length;
    }

    public static void addEntry(String topic, int offset, int len) {
        Entry entry = new Entry(offset, len);
        indexes.add(topic, entry);
        mappings.computeIfAbsent(topic, k -> new HashMap<>()).put(offset, entry);
    }

    public static List<Entry> getEntries(String topic) {
        return indexes.get(topic);
    }

    public static Entry getEntry(String topic, int offset) {
        Map<Integer, Entry> map = mappings.get(topic);
        return map == null ? null : map.get(offset);
    }
}
