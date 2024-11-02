package edu.smu.smusql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// BTreeNode class that represents a node in the B-tree
class BTreeNode<T extends Comparable<T>> {
    int t;  // Minimum degree
    List<T> keys;
    List<Map<String, Object>> values;
    List<BTreeNode<T>> children;
    boolean isLeaf;

    public BTreeNode(int t, boolean isLeaf) {
        this.t = t;
        this.isLeaf = isLeaf;
        this.keys = new ArrayList<>(2 * t - 1);
        this.values = new ArrayList<>(2 * t - 1);
        this.children = new ArrayList<>(2 * t);
    }

    public void insertNonFull(T key, Map<String, Object> value) {
        int i = keys.size() - 1;

        if (isLeaf) {
            while (i >= 0 && keys.get(i).compareTo(key) > 0) {
                i--;
            }
            keys.add(i + 1, key);
            values.add(i + 1, value);
        } else {
            while (i >= 0 && keys.get(i).compareTo(key) > 0) {
                i--;
            }
            BTreeNode<T> child = children.get(i + 1);
            if (child.keys.size() == 2 * t - 1) {
                splitChild(i + 1, child);
                if (keys.get(i + 1).compareTo(key) < 0) {
                    i++;
                }
            }
            children.get(i + 1).insertNonFull(key, value);
        }
    }

    public void splitChild(int i, BTreeNode<T> y) {
        // Create a new node z to store the second half of y
        BTreeNode<T> z = new BTreeNode<>(y.t, y.isLeaf);
    
        // Move the second half of y's keys and values to z
        for (int j = 0; j < t - 1; j++) {
            z.keys.add(y.keys.remove(t));  // Move the key to z
            z.values.add(y.values.remove(t));  // Move the value to z
        }
    
        // If y is not a leaf, move the second half of y's children to z
        if (!y.isLeaf) {
            for (int j = 0; j < t; j++) {
                z.children.add(y.children.remove(t));
            }
        }
    
        // Insert z as a child of the current node
        children.add(i + 1, z);
    
        // Move the middle key and value of y up to the parent node (this node)
        keys.add(i, y.keys.remove(t - 1));
        values.add(i, y.values.remove(t - 1));
    }

    public void rangeQuery(T lowerBound, T upperBound, List<Map<String, Object>> result) {
        int i = 0;
        while (i < keys.size() && keys.get(i).compareTo(lowerBound) < 0) {
            i++;
        }
        while (i < keys.size() && keys.get(i).compareTo(upperBound) <= 0) {
            if (!isLeaf) {
                children.get(i).rangeQuery(lowerBound, upperBound, result);
            }
            result.add(values.get(i));
            i++;
        }
        if (!isLeaf) {
            children.get(i).rangeQuery(lowerBound, upperBound, result);
        }
    }

    public List<Map<String, Object>> search(T key) {
        int i = 0;
    
        // Find the first key greater than or equal to the key we are searching for
        while (i < keys.size() && key.compareTo(keys.get(i)) > 0) {
            i++;
        }
    
        // If the found key is equal to the key, we return the associated value(s)
        if (i < keys.size() && keys.get(i).compareTo(key) == 0) {
            return List.of(values.get(i));
        }

        if (isLeaf) {
            return null;
        }
    
        return children.get(i).search(key);
    }

}

public class BTree<T extends Comparable<T>> {
    private BTreeNode<T> root;
    private int t;

    public BTree(int t) {
        this.root = null;
        this.t = t;
    }

    public void insert(T key, Map<String, Object> value) {
        if (root == null) {
            root = new BTreeNode<>(t, true);
            root.keys.add(key);
            root.values.add(value);
        } else {
            if (root.keys.size() == 2 * t - 1) {
                BTreeNode<T> s = new BTreeNode<>(t, false);
                s.children.add(root);
                s.splitChild(0, root);
                root = s;
            }
            root.insertNonFull(key, value);
        }
    }

    public List<Map<String, Object>> rangeQuery(T lowerBound, T upperBound) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (root != null) {
            root.rangeQuery(lowerBound, upperBound, result);
        }
        return result;
    }
    public List<Map<String, Object>> search(T key) {
        if (root != null) {
            return root.search(key);
        }
        return null;
    }
}

