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
        BTreeNode<T> z = new BTreeNode<>(y.t, y.isLeaf);
        z.keys = new ArrayList<>(y.keys.subList(t, 2 * t - 1));
        z.values = new ArrayList<>(y.values.subList(t, 2 * t - 1));
        if (!y.isLeaf) {
            z.children = new ArrayList<>(y.children.subList(t, 2 * t));
        }
        y.keys = new ArrayList<>(y.keys.subList(0, t - 1));
        y.values = new ArrayList<>(y.values.subList(0, t - 1));
        children.add(i + 1, z);
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
}

// BTree class
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
}

