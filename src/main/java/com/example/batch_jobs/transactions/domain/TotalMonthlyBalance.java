package com.example.batch_jobs.transactions.domain;

import java.math.BigDecimal;

public record TotalMonthlyBalance(
        String month,
        BigDecimal balance
) {
}
