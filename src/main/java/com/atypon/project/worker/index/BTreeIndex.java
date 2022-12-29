package com.atypon.project.worker.index;

import com.fasterxml.jackson.databind.JsonNode;
import com.atypon.project.worker.core.Entry;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class BTreeIndex implements Index {

    private static final long serialVersionUID = 13L;

    BTreeNode root;
    Comparator<JsonNode> comparator;
    int degree; // minimum degree

    public BTreeIndex(int degree, Comparator<JsonNode> comparator) {
        this.root = null;
        this.comparator = comparator;
        this.degree = degree;
    }

    @Override
    public List<String> search(JsonNode k) {
        BTreeNode node = searchNode(k);
        if(node == null) {
            return new ArrayList<>();
        }
        return Arrays
                .stream(node.keys)
                .filter(entry -> entry != null && entry.getKey().equals(k))
                .flatMap(entry -> entry.getValue().stream())
                .collect(Collectors.toList());
    }

    @Override
    public void add(JsonNode key, String value) {
        // case of a tree is empty
        BTreeNode node = searchNode(key);
        if(node != null) {
            for(Entry<JsonNode, List<String>> entry: node.keys) {
                if(!entry.getKey().equals(key))
                    continue;
                List<String> list = entry.getValue();
                if(!list.contains(value))
                    list.add(value);
                break;
            }
            return;
        }

        if (root == null) {
            root = new BTreeNode(degree, true);
            List<String> list = new ArrayList<>();
            list.add(value);
            root.keys[0] = new Entry<>(key, list);
            root.numNodes = 1;
        } else {

            if (root.numNodes == 2* degree -1) {
                BTreeNode s = new BTreeNode(degree, false);
                s.C[0] = root;
                s.splitChild(0, root);
                int i = 0;
                if (less(s.keys[0].getKey(), key))
                    i++;
                s.C[i].insertNonFull(key, value);

                root = s;
            } else
                root.insertNonFull(key, value);
        }
    }

    @Override
    public void delete(JsonNode key, String value) {
        // case of an empty tree
        if (root == null)
            return;

        BTreeNode node = searchNode(key);
        boolean doRemove = true;
        if(node != null) {
            for(Entry<JsonNode, List<String>> entry: node.keys) {
                // entry doesn't have value
                if(!entry.getKey().equals(key))
                    continue;
                List<String> list = entry.getValue();
                if(list.contains(value))
                    list.remove(value);
                doRemove = list.isEmpty();
                break;
            }

        }
        if(!doRemove)
            return;
        root.remove(key);

        if (root.numNodes ==0) {
            BTreeNode tmp = root;
            if (root.isLeaf)
                root = null;
            else
                root = root.C[0];
        }
        return;
    }

    @Override
    public boolean contains(JsonNode k)  {
        return searchNode(k) != null;
    }

    @Override
    public void clear() {
        root = null;
    }

    public boolean contains(JsonNode key, String value)  {
        BTreeNode node = searchNode(key);
        if(node != null) {
            for(Entry<JsonNode, List<String>> entry: node.keys) {
                if(!entry.getKey().equals(key))
                    continue;
                List<String> list = entry.getValue();
                if(list.contains(value))
                    return true;
                break;
            }
        }

        return false;
    }

    // function to search a key in this tree
    public BTreeNode searchNode(JsonNode k)  {
        return (root == null) ? null : root.search(k);
    }

    public void traverse() {
        if (root != null) root.traverse();
        System.out.println();
    }

    private boolean less(JsonNode o1, JsonNode o2) {
        return comparator.compare(o1, o2) < 0;
    }


    private class BTreeNode implements Serializable {

        private static final long serialVersionUID = 5L;

        Entry<JsonNode, List<String>> keys[];  // An array of keys
        int degree;      // Minimum degree (defines the range for number of keys)
        BTreeNode[] C; // An array of children
        int numNodes;     // Current number of keys
        boolean isLeaf; // Is true when node is leaf. Otherwise false


        public BTreeNode(int t1, boolean leaf1) {
            degree = t1;
            isLeaf = leaf1;

            keys = new Entry[2* degree -1];
            C = new BTreeNode[2* degree];
            numNodes = 0;
        }

        private boolean less(JsonNode o1, JsonNode o2) {
            return comparator.compare(o1, o2) < 0;
        }

        private boolean greater(JsonNode o1, JsonNode o2) {
            return comparator.compare(o1, o2) > 0;
        }

        // utility function that returns the index of the first key that is
        // greater than or equal to k
        public int findKey(JsonNode key) {
            int idx=0;
            while (idx< numNodes && less(keys[idx].getKey(), key))
                ++idx;
            return idx;
        }

        public void remove(JsonNode key) {
            int idx = findKey(key);

            // the key to be removed is present in this node
            if (idx < numNodes && keys[idx].getKey().equals(key)) {
                if (isLeaf)
                    removeFromLeaf(idx);
                else
                    removeFromNonLeaf(idx);
            } else {
                // if this node is a leaf node, then the key is not present in tree
                if (isLeaf) { // key not present in the tree
                    return;
                }

                boolean isPresent = ( (idx== numNodes)? true : false );

                if (C[idx].numNodes < degree)
                    fill(idx);

                if (isPresent && idx > numNodes)
                    C[idx-1].remove(key);
                else
                    C[idx].remove(key);
            }
            return;
        }

        // function to remove the idx-th key from this node - which is a leaf node
        public void removeFromLeaf(int idx) {

            // move all the keys after the idx-th pos one place backward
            for (int i = idx+1; i< numNodes; ++i)
                keys[i-1] = keys[i];

            // reduce the count of keys
            numNodes--;

            return;
        }

        // function to remove the idx-th key from this node - which is a non-leaf node
        public void removeFromNonLeaf(int idx) {

            Entry<JsonNode, List<String>> k = keys[idx];

            if (C[idx].numNodes >= degree) {
                Entry<JsonNode, List<String>> predecessor = getPredecessor(idx);
                keys[idx] = predecessor;
                C[idx].remove(predecessor.getKey());
            }


            else if  (C[idx+1].numNodes >= degree) {
                Entry<JsonNode, List<String>> successor = getSuccessor(idx);
                keys[idx] = successor;
                C[idx+1].remove(successor.getKey());
            }

            else {
                merge(idx);
                C[idx].remove(k.getKey());
            }
            return;
        }

        // function to get predecessor of keys[idx]
        public Entry<JsonNode, List<String>> getPredecessor(int idx)
        {
            // keep moving to the right most node until we reach a leaf
            BTreeNode cur=C[idx];
            while (!cur.isLeaf)
                cur = cur.C[cur.numNodes];

            // return the last key of the leaf
            return cur.keys[cur.numNodes -1];
        }

        public Entry<JsonNode, List<String>> getSuccessor(int idx)
        {

            // keep moving the left most node starting from C[idx+1] until we reach a leaf
            BTreeNode cur = C[idx+1];
            while (!cur.isLeaf)
                cur = cur.C[0];

            // return the first key of the leaf
            return cur.keys[0];
        }

        // A function to fill child C[idx] which has less than t-1 keys
        public void fill(int idx) {

            if (idx!=0 && C[idx-1].numNodes >= degree)
                borrowFromPrev(idx);
            else if (idx!= numNodes && C[idx+1].numNodes >= degree)
                borrowFromNext(idx);
            else {
                if (idx != numNodes)
                    merge(idx);
                else
                    merge(idx-1);
            }
            return;
        }

        public void borrowFromPrev(int idx) {

            BTreeNode child=C[idx];
            BTreeNode sibling=C[idx-1];


            // moving all key in C[idx] one step ahead
            for (int i = child.numNodes -1; i>=0; --i)
                child.keys[i+1] = child.keys[i];

            // iff C[idx] is not a leaf, move all its child pointers one step ahead
            if (!child.isLeaf) {
                for(int i = child.numNodes; i>=0; --i)
                    child.C[i+1] = child.C[i];
            }

            child.keys[0] = keys[idx-1];

            // moving sibling's last child as C[idx]'s first child
            if(!child.isLeaf)
                child.C[0] = sibling.C[sibling.numNodes];

            keys[idx-1] = sibling.keys[sibling.numNodes -1];

            child.numNodes += 1;
            sibling.numNodes -= 1;

            return;
        }
        public void borrowFromNext(int idx) {

            BTreeNode child=C[idx];
            BTreeNode sibling=C[idx+1];

            child.keys[(child.numNodes)] = keys[idx];

            if (!(child.isLeaf))
                child.C[(child.numNodes)+1] = sibling.C[0];

            keys[idx] = sibling.keys[0];

            for (int i = 1; i<sibling.numNodes; ++i)
                sibling.keys[i-1] = sibling.keys[i];

            if (!sibling.isLeaf) {
                for(int i = 1; i<=sibling.numNodes; ++i)
                    sibling.C[i-1] = sibling.C[i];
            }
            child.numNodes += 1;
            sibling.numNodes -= 1;
            return;
        }


        public void merge(int idx) {
            BTreeNode child = C[idx];
            BTreeNode sibling = C[idx+1];

            child.keys[degree -1] = keys[idx];

            for (int i = 0; i<sibling.numNodes; ++i)
                child.keys[i+ degree] = sibling.keys[i];

            if (!child.isLeaf)
            {
                for(int i = 0; i<=sibling.numNodes; ++i)
                    child.C[i+ degree] = sibling.C[i];
            }

            for (int i = idx+1; i< numNodes; ++i)
                keys[i-1] = keys[i];

            for (int i = idx+2; i<= numNodes; ++i)
                C[i-1] = C[i];

            child.numNodes += sibling.numNodes +1;
            numNodes--;

            return;
        }


        public void insertNonFull(JsonNode key, String value) {
            int i = numNodes -1;

            if (isLeaf == true) {
                while (i >= 0 && greater(keys[i].getKey(), key)) {
                    keys[i+1] = keys[i];
                    i--;
                }
                List<String> list = new ArrayList<>();
                list.add(value);
                // insert the new key at found location
                keys[i+1] = new Entry<>(key, list);
                numNodes = numNodes +1;
            }
            else {

                while (i >= 0 && greater(keys[i].getKey(), key))
                    i--;


                if (C[i+1].numNodes == 2* degree -1) {

                    splitChild(i+1, C[i+1]);

                    if (less(keys[i+1].getKey(), key))
                        i++;
                }
                C[i+1].insertNonFull(key, value);
            }
        }

        public void splitChild(int i, BTreeNode y) {

            BTreeNode z = new BTreeNode(y.degree, y.isLeaf);
            z.numNodes = degree - 1;

            for (int j = 0; j < degree -1; j++)
                z.keys[j] = y.keys[j+ degree];

            if (y.isLeaf == false) {
                for (int j = 0; j < degree; j++)
                    z.C[j] = y.C[j+ degree];
            }

            y.numNodes = degree - 1;

            for (int j = numNodes; j >= i+1; j--)
                C[j+1] = C[j];

            C[i+1] = z;

            for (int j = numNodes -1; j >= i; j--)
                keys[j+1] = keys[j];

            keys[i] = y.keys[degree -1];

            numNodes = numNodes + 1;
        }

        public void traverse() {

            int i;
            for (i = 0; i < numNodes; i++) {

                if (isLeaf == false)
                    C[i].traverse();
                System.out.print(" " + keys[i]);
            }

            if (isLeaf == false)
                C[i].traverse();
        }

        public BTreeNode search(JsonNode k) {

            int i = 0;
            try {
                while (i < numNodes && greater(k, keys[i].getKey()))
                    i++;

                if (i < numNodes && keys[i].getKey().equals(k))
                    return this;

                if (isLeaf == true)
                    return null;

                return C[i].search(k);
            } catch (NullPointerException e) {
                throw e;
            }
        }
    }

}
