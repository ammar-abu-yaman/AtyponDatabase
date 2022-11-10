package com.atypon.project.worker.core;

import java.io.Serializable;
import java.util.Objects;

public class Node implements Serializable {

    private final String id;
    private final String address;
    private int numDocuments;

    public Node(String id, String address, int numDocuments) {
        this.id = id;
        this.address = address;
        this.numDocuments = numDocuments;
    }

    public String getId() {
        return id;
    }

    public String getAddress() {
        return address;
    }

    public int getNumDocuments() {
        return numDocuments;
    }

    public void setNumDocuments(int numDocuments) {
        this.numDocuments = numDocuments;
    }

    public void incNumDocuments() {
        numDocuments++;
    }

    @Override
    public String toString() {
        return "Node{" +
                "id='" + id + '\'' +
                ", address='" + address + '\'' +
                ", numDocuments=" + numDocuments +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return numDocuments == node.numDocuments && id.equals(node.id) && address.equals(node.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, address, numDocuments);
    }
}
