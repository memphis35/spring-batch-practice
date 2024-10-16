package com.example.batch_jobs.transactions.domain;

import java.math.BigDecimal;

public record TotalBalance(
        Integer id,
        BigDecimal total
) {
}
