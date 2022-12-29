package com.atypon.project.worker;

import com.atypon.project.worker.index.BTreeIndex;
import com.atypon.project.worker.index.Index;
import com.atypon.project.worker.index.JsonComparator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class IndexTests {
    @Test
    public void testInsertion() {
        Index index = new BTreeIndex(5, new JsonComparator());
        JsonNode key = new ObjectMapper().valueToTree("\"text\"");
        index.add(key, "1");
        index.add(key, "2");
        index.add(key, "3");
        Assertions.assertTrue(index.contains(key));
        Assertions.assertEquals(index.search(key), Arrays.asList("1", "2", "3"));
    }

    @Test
    void testRemoval() {
        Index index = new BTreeIndex(5, new JsonComparator());
        JsonNode key = new ObjectMapper().valueToTree("\"text\"");
        index.add(key, "1");
        index.add(key, "2");
        index.add(key, "3");
        index.delete(key, "2");
        Assertions.assertTrue(index.contains(key));
        Assertions.assertEquals(index.search(key), Arrays.asList("1", "3"));
    }

    @Test
    void testClear() {
        Index index = new BTreeIndex(5, new JsonComparator());
        JsonNode key = new ObjectMapper().valueToTree("\"text\"");
        index.add(key, "1");
        index.clear();
        Assertions.assertFalse(index.contains(key));
    }
}
