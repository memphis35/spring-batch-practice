package com.example.batch_jobs.transactions.domain;

import java.math.BigDecimal;

public record TotalMerchantBalance(
        String merchant,
        BigDecimal balance
) {
}
