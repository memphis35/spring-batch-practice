package com.example.batch_jobs.coins;

import com.example.batch_jobs.coins.domain.Coin;
import com.example.batch_jobs.coins.domain.CoinType;
import com.example.batch_jobs.coins.domain.PlayerScore;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.NonNull;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.HashMap;
import java.util.Map;

public abstract class GeneralCoinsJobConfiguration {

    protected static final RowMapper<Coin> COIN_ROW_MAPPER = (rs, rowNum) -> new Coin(
            rs.getString(2),
            CoinType.valueOf(rs.getString(3).toUpperCase()),
            rs.getBigDecimal(4));

    @Bean
    public ItemReader<Coin> coinTablePagingItemReader(DataSource dataSource) {

        return new JdbcPagingItemReaderBuilder<Coin>()
                .name("coin_table_paging_item_reader")
                .dataSource(dataSource)
                .fetchSize(20)
                .selectClause("id, player_name, coin_type, coin_score")
                .fromClause("coins.collected_coin")
                .sortKeys(Map.of("id", Order.ASCENDING))
                .rowMapper(COIN_ROW_MAPPER)
                .build();
    }

    @Bean
    public ItemProcessor<Coin, PlayerScore> coinScoreAggregator() {
        return new ItemProcessor<>() {
            private final Map<String, BigDecimal> result = new HashMap<>();

            @Override
            public PlayerScore process(@NonNull Coin item) {
                final String name = item.player();
                final BigDecimal currentScore = result.getOrDefault(name, BigDecimal.ZERO);
                final BigDecimal calculated = item.type().equals(CoinType.ADD)
                        ? currentScore.add(item.score())
                        : currentScore.multiply(item.score(), new MathContext(1));
                result.put(name, calculated);
                return new PlayerScore(name, calculated);
            }
        };
    }

    @Bean
    public ItemWriter<PlayerScore> playerScoreItemWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<PlayerScore>()
                .dataSource(dataSource)
                .sql("""
                        INSERT INTO coins.player_score
                        VALUES (?, ?)
                        ON CONFLICT (player_name) DO UPDATE
                        SET total_score = ?
                        """)
                .itemPreparedStatementSetter((item, ps) -> {
                    ps.setString(1, item.name());
                    ps.setBigDecimal(2, item.total());
                    ps.setBigDecimal(3, item.total());
                }).build();
    }
}
