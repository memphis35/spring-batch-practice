package com.example.batch_jobs.coins.domain;

import java.math.BigDecimal;

public record Coin(
        String player,
        CoinType type,
        BigDecimal score
) {
}
