package com.atypon.project.worker.cache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SyncLRUCache<K, V> implements Cache<K, V> {

    private long capacity = Long.MAX_VALUE; // unbounded by default
    private Map<K, Node> references;
    private DoublyLinkedList queue;

    public SyncLRUCache() {
        this.references = new HashMap<>();
        this.queue = new DoublyLinkedList();
    }

    public SyncLRUCache(long capacity) {
        this();
        this.capacity = capacity;
    }

    @Override
    public synchronized Optional<V> get(K key) {
        if(!contains(key))
            return Optional.empty();

        Node node = references.get(key);
        K nodeKey = node.key;
        V nodeValue = node.value;

        queue.remove(node);
        references.put(nodeKey, queue.addFirst(nodeKey, nodeValue));

        return Optional.of(nodeValue);
    }

    @Override
    public synchronized void put(K key, V value) {
        if(contains(key)) {
            get(key); // move entry to front of the queue
            references.get(key).value = value;
            return;
        }

        if(size() >= capacity())
            evict();
        references.put(key, queue.addFirst(key, value));
    }

    @Override
    public synchronized boolean contains(K key) {
        return references.containsKey(key);
    }

    @Override
    public synchronized void remove(K key) {
        Node node = references.get(key);
        queue.remove(node);
        references.remove(key);
    }

    @Override
    public synchronized void removeIf(Function<K, Boolean> filter) {
        List<K> entriesToRemove = references.keySet()
                .stream()
                .filter(key -> filter.apply(key))
                .collect(Collectors.toList());
        for(K entry: entriesToRemove)
            remove(entry);
    }

    @Override
    public synchronized int size() {
        return references.size();
    }

    @Override
    public long capacity() {
        return capacity;
    }

    @Override
    public void clear() {
        references.clear();
        queue.clear();
    }


    private void evict() {
        Node lruNode = queue.getLast();
        references.remove(lruNode.key);
        queue.remove(lruNode);
    }


    private class DoublyLinkedList {
        private Node head, tail;

        public DoublyLinkedList() {
            this.head = new Node();
            this.tail = new Node();

            this.head.attach(this.tail);
        }

        public Node addFirst(K key, V value) {
            Node node = new Node(key, value, head, head.next);
            return node;
        }

        public Node addLast(K key, V value) {
            Node node = new Node(key, value, tail.prev, tail);
            return node;
        }

        public Node getFirst() {
            return this.head.next;
        }

        public Node getLast() {
            return this.tail.prev;
        }

        public void remove(Node node) {
            node.detach();
        }

        public void clear() {
            this.head.attach(tail);
        }

        @Override
        public String toString() {
            return "DoublyLinkedList{\n" +
                    "\nhead=" + head +
                    "\n, tail=" + tail +
                    "\n}";
        }
    }

    @Override
    public String toString() {
        return "SyncLRUCache{\n" +
                "capacity=" + capacity +
                "\n, references=" + references +
                "\n, queue=" + queue +
                "\n}";
    }

    private class Node {
        public K key;
        public V value;
        public Node prev, next;

        public Node() {}

        public Node(K key, V value, Node prev, Node next) {
            this.key = key;
            this.value = value;
            this.prev = prev;
            this.next = next;

            this.attachBetween(prev, next);
        }

        public void attach(Node right) {
            this.next = right;
            right.prev = this;
        }

        public void attachBetween(Node left, Node right) {
            left.next = this;
            this.prev = left;
            this.next = right;
            right.prev = this;
        }

        public void detach() {
            if(this.prev == null && this.next == null) {
                return;
            }
            if (this.prev != null && this.next != null) {
                this.prev.attach(this.next);
                this.prev = null;
                this.next = null;
                return;
            }

            if(this.next == null) {
                this.next.prev = null;
                this.next = null;
                return;
            }

            if(this.prev == null) {
                this.prev.next = null;
                this.prev = null;
                return;
            }
        }

        @Override
        public String toString() {
            String out = "Node {";
            Node curr = this;
            while(curr != null) {
                out += "[" + curr.key + "=" + curr.value + "] ";
                curr = curr.next;
            }
            out += "}";
            return out;
        }
    }

}
