package com.example.batch_jobs.transactions.domain;

import org.springframework.lang.NonNull;

import java.math.BigDecimal;
import java.sql.Date;

public record MerchantTransaction(
        Integer id,
        Date datetime,
        @NonNull BigDecimal amount,
        String merchant
) {
}
