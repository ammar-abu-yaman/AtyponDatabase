package com.atypon.project.worker.cache;

import java.util.Optional;
import java.util.function.Function;

public interface Cache <K, V> {
    public Optional<V> get(K key);
    public void put(K key, V value);
    public boolean contains(K key);
    public void remove(K key);
    public void removeIf(Function<K, Boolean> filter);
    public int size();
    public long capacity();
    public void clear();
}
