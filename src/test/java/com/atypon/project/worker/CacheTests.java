package com.atypon.project.worker;

import com.atypon.project.worker.cache.Cache;
import com.atypon.project.worker.cache.SyncLRUCache;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CacheTests {

    @Test
    public void testInsertions() {
        Cache<String, Integer> cache = new SyncLRUCache<>(3);
        cache.put("A", 1);
        cache.put("B", 2);
        Assertions.assertTrue(cache.contains("A"));
    }

    @Test
    public void testDeletion() {
        Cache<String, Integer> cache = new SyncLRUCache<>(3);
        cache.put("A", 1);
        cache.put("B", 2);
        cache.remove("B");
        Assertions.assertFalse(cache.contains("B"));
    }

    @Test
    public void testLRUEviction() {
        Cache<String, Integer> cache = new SyncLRUCache<>(3);
        cache.put("A", 1);
        cache.put("B", 2);
        cache.put("C", 3);
        cache.put("D", 4);
        Assertions.assertFalse(cache.contains("A"));
    }

}
