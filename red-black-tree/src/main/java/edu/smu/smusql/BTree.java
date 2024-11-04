package edu.smu.smusql;

import java.util.ArrayList;
import java.util.List;

// BTreeNode class that represents a node in the B-tree

/*
 *with a minimum degree t a node can hold up to 2t -1 key and 2t children 
*when splitting the middle key (t -1 ) should move up and the node is divided around this key
 * when splitting the node y should retain the first t-1 key and last t-1 key is move to new node z
 */
class BTreeNode<T extends Comparable<T>> {
    int t; // Minimum degree
    List<T> keys;
    List<String> values; // store primary key references as string
    List<BTreeNode<T>> children;
    boolean isLeaf;

    public BTreeNode(int t, boolean isLeaf) {
        this.t = t;
        this.isLeaf = isLeaf;
        this.keys = new ArrayList<>(2 * t - 1);
        this.values = new ArrayList<>(2 * t - 1);
        this.children = new ArrayList<>(2 * t);
    }

    public void insertNonFull(T key, String primaryKeyReference) {
        int i = keys.size() - 1;

        if (isLeaf) {
            // Find the location to insert the new key
            while (i >= 0 && keys.get(i).compareTo(key) > 0) {
                i--;
            }
            keys.add(i + 1, key);
            values.add(i + 1, primaryKeyReference);
        } else {
            // Locate the child which is going to have the new key
            while (i >= 0 && keys.get(i).compareTo(key) > 0) {
                i--;
            }
            BTreeNode<T> child = children.get(i + 1);

            // If the child is full, split it
            if (child.keys.size() == 2 * t - 1) {
                splitChild(i + 1, child);
                if (keys.get(i + 1).compareTo(key) < 0) {
                    i++;
                }
            }
            children.get(i + 1).insertNonFull(key, primaryKeyReference);
        }
    }

    public void splitChild(int i, BTreeNode<T> y) {
        // Verify that `y` has enough keys for the split
        BTreeNode<T> z = new BTreeNode<>(y.t, y.isLeaf);

        // Transfer the last (t - 1) keys and values from `y` to `z`
        for (int j = 0; j < t - 1; j++) {
            z.keys.add(y.keys.remove(t));
            z.values.add(y.values.remove(t));
        }

        // If `y` is not a leaf, move `t` children from `y` to `z`

        if (!y.isLeaf) {
            for (int j = 0; j < t; j++) {
                z.children.add(y.children.remove(t));
            }
        }

        // Insert `z` into `children` at index `i + 1`
        children.add(i + 1, z);

        // Move the middle key and value of `y` up to this node
        keys.add(i, y.keys.remove(t - 1));
        values.add(i, y.values.remove(t - 1));
    }

    public void rangeQuery(T lowerBound, T upperBound, List<String> result) {
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

    public void rangeQueryExclusive(T lowerBound, T upperBound, List<String> result) {
        int i = 0;
        while (i < keys.size() && (lowerBound == null || keys.get(i).compareTo(lowerBound) > 0)) {
            i++;
        }
        while (i < keys.size() && (upperBound == null || keys.get(i).compareTo(upperBound) < 0)) {
            if (!isLeaf) {
                children.get(i).rangeQueryExclusive(lowerBound, upperBound, result);
            }
            result.add(values.get(i));
            i++;
        }
        if (!isLeaf && i < children.size()) {
            children.get(i).rangeQueryExclusive(lowerBound, upperBound, result);
        }
    }
}

public class BTree<T extends Comparable<T>> {
    private BTreeNode<T> root;
    private int t;

    public BTree(int t) {
        this.root = null;
        this.t = t;
    }

    public void insert(T key, String primaryKeyReference) {
        if (root == null) {
            root = new BTreeNode<>(t, true);
            root.keys.add(key);
            root.values.add(primaryKeyReference);
        } else {
            if (root.keys.size() == 2 * t - 1) {
                BTreeNode<T> s = new BTreeNode<>(t, false);
                s.children.add(root);
                s.splitChild(0, root);
                root = s;
            }
            root.insertNonFull(key, primaryKeyReference);
        }
    }

    public List<String> search(T key) {
        return root == null ? new ArrayList<>() : searchRecursive(root, key);
    }

    private List<String> searchRecursive(BTreeNode<T> node, T key) {
        int i = 0;
        while (i < node.keys.size() && key.compareTo(node.keys.get(i)) > 0) {
            i++;
        }

        if (i < node.keys.size() && node.keys.get(i).equals(key)) {
            List<String> result = new ArrayList<>();
            result.add(node.values.get(i));
            return result;
        } else if (node.isLeaf) {
            return new ArrayList<>(); // Key not found
        } else {
            return searchRecursive(node.children.get(i), key);
        }
    }

    public List<String> rangeSearch(T key, String operator) {
        List<String> result = new ArrayList<>();
        if (root != null) {
            switch (operator) {
                case ">":
                    root.rangeQueryExclusive(key, null, result);
                    break;
                case ">=":
                    root.rangeQuery(key, null, result);
                    break;
                case "<":
                    root.rangeQueryExclusive(null, key, result);
                    break;
                case "<=":
                    root.rangeQuery(null, key, result);
                    break;
            }
        }
        return result;
    }

    public List<String> rangeQuery(T lowerBound, T upperBound) {
        List<String> result = new ArrayList<>();
        if (root != null) {
            root.rangeQuery(lowerBound, upperBound, result);
        }
        return result;
    }
}
