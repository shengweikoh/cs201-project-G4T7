package edu.smu.smusql;

import java.util.ArrayList;
import java.util.List;

public class TransactionLog {
    private List<Transaction> transactions = new ArrayList<>();

    public void addTransaction(Transaction transaction) {
        transactions.add(transaction);
    }

    public Transaction getLastTransaction() {
        return transactions.isEmpty() ? null : transactions.remove(transactions.size() - 1);
    }
}