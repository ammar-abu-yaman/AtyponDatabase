package com.atypon.project.worker.cache;

import java.util.Optional;
import java.util.function.Function;

public interface Cache <K, V> {
    Optional<V> get(K key);
    void put(K key, V value);
    boolean contains(K key);
    void remove(K key);
    void removeIf(Function<K, Boolean> filter);
    int size();
    long capacity();
    void clear();
}
