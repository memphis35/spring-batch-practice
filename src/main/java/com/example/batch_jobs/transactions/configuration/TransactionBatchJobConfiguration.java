package com.example.batch_jobs.transactions.configuration;

import com.example.batch_jobs.transactions.domain.MerchantTransaction;
import com.example.batch_jobs.transactions.domain.TotalBalance;
import com.example.batch_jobs.transactions.domain.TotalMerchantBalance;
import com.example.batch_jobs.transactions.domain.TotalMonthlyBalance;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.json.GsonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonObjectMarshaller;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.WritableResource;
import org.springframework.lang.NonNull;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.util.Map;

@Configuration
@Profile("transactions")
public class TransactionBatchJobConfiguration {

    @Bean
    public Job analyseAndReportTransactions(@Qualifier("calculateAndAnalyseBalance") Step calculateBalance,
                                            @Qualifier("aggregateBalanceByMerchant") Step aggregateByMerchant,
                                            @Qualifier("aggregateBalanceByMonth") Step aggregateByMonth,
                                            JobRepository jobRepository) {
        return new JobBuilder("bank_transaction_analysis_and_report", jobRepository)
                .flow(calculateBalance).on("POSITIVE").to(aggregateByMerchant)
                .from(calculateBalance).on("NEGATIVE").to(aggregateByMonth)
                .from(calculateBalance).on("*").end()
                .build()
                .build();
    }

    @Bean
    public Step calculateAndAnalyseBalance(@Qualifier("balanceColumnStepExecutionListener") StepExecutionListener addBalanceColumn,
                                           @Qualifier("transactionItemReader") ItemReader<MerchantTransaction> reader,
                                           @Qualifier("sequentialBalanceProcessor") SequentialBalanceProcessor processor,
                                           @Qualifier("balanceItemWriter") ItemWriter<TotalBalance> writer,
                                           JobRepository jobRepository,
                                           PlatformTransactionManager platformTransactionManager) {
        return new StepBuilder("calculate_total_balance", jobRepository)
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(@NonNull StepExecution stepExecution) {
                        processor.setStepExecution(stepExecution);
                    }

                    @Override
                    public ExitStatus afterStep(@NonNull StepExecution stepExecution) {
                        processor.setStepExecution(null);
                        final BigDecimal currentBalance = stepExecution.getExecutionContext().get("currentBalance", BigDecimal.class);
                        final String status = currentBalance.compareTo(BigDecimal.ZERO) > 0 ? "POSITIVE" : "NEGATIVE";
                        stepExecution.getExecutionContext().remove("currentBalance");
                        return new ExitStatus(status);
                    }
                })
                .listener(addBalanceColumn)
                .<MerchantTransaction, TotalBalance>chunk(20, platformTransactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public ItemReader<MerchantTransaction> transactionItemReader(@Qualifier("dataSource") DataSource dataSource) {
        return new JdbcCursorItemReaderBuilder<MerchantTransaction>()
                .name("transaction_item_reader")
                .dataSource(dataSource)
                .sql("SELECT id, datetime, amount, merchant FROM bank.transaction ORDER BY datetime, merchant, amount DESC")
                .rowMapper((rs, rowNum) -> {
                    final var id = rs.getObject("id", Integer.class);
                    final var amount = rs.getBigDecimal("amount");
                    return new MerchantTransaction(id, null, amount, null);
                })
                .saveState(false)
                .build();
    }

    @Bean
    public ItemWriter<TotalBalance> balanceItemWriter(@Qualifier("dataSource") DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<TotalBalance>()
                .dataSource(dataSource)
                .sql("UPDATE bank.transaction SET balance = ? WHERE id = ?")
                .itemPreparedStatementSetter((item, ps) -> {
                    ps.setBigDecimal(1, item.total());
                    ps.setInt(2, item.id());
                })
                .build();
    }

    @Bean
    public Step aggregateBalanceByMerchant(@Qualifier("pagedByMerchantTransactionReader") ItemReader<TotalMerchantBalance> reader,
                                        @Qualifier("merchantTotalBalanceWriter") ItemWriter<TotalMerchantBalance> writer,
                                        JobRepository jobRepository,
                                        PlatformTransactionManager platformTransactionManager) {
        return new StepBuilder("aggregate_balance_by_merchant", jobRepository)
                .<TotalMerchantBalance, TotalMerchantBalance>chunk(5, platformTransactionManager)
                .reader(reader)
                .writer(writer)
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public ItemReader<TotalMerchantBalance> pagedByMerchantTransactionReader(@Qualifier("dataSource") DataSource dataSource) {
        return new JdbcPagingItemReaderBuilder<TotalMerchantBalance>()
                .name("paged_transaction_item_reader")
                .dataSource(dataSource)
                .selectClause("SELECT merchant, sum(amount) as balance")
                .fromClause("FROM bank.transaction")
                .groupClause("GROUP BY merchant")
                .sortKeys(Map.of("balance", Order.DESCENDING))
                .pageSize(10)
                .rowMapper((rs, rowNum) -> {
                    final var merchant = rs.getString("merchant");
                    final var balance = rs.getBigDecimal("balance");
                    return new TotalMerchantBalance(merchant, balance);
                }).build();
    }

    @Bean
    public ItemWriter<TotalMerchantBalance> merchantTotalBalanceWriter(@Value("file:total_merchant_balance.json") WritableResource destination,
                                                                       JsonObjectMarshaller<TotalMerchantBalance> marshaller) {
        return new JsonFileItemWriterBuilder<TotalMerchantBalance>()
                .name("json_total_merchant_balance_writer")
                .resource(destination)
                .jsonObjectMarshaller(marshaller)
                .build();
    }

    @Bean
    public JsonObjectMarshaller<TotalMerchantBalance> merchantBalanceMarshaller() {
        return new GsonJsonObjectMarshaller<>();
    }

    @Bean
    public Step aggregateBalanceByMonth(@Qualifier("pagedByMonthTransactionReader") ItemReader<TotalMonthlyBalance> reader,
                                        @Qualifier("monthTotalBalanceWriter") ItemWriter<TotalMonthlyBalance> writer,
                                        JobRepository jobRepository,
                                        PlatformTransactionManager platformTransactionManager) {
        return new StepBuilder("aggregate_balance_by_month", jobRepository)
                .<TotalMonthlyBalance, TotalMonthlyBalance>chunk(5, platformTransactionManager)
                .reader(reader)
                .writer(writer)
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public ItemReader<TotalMonthlyBalance> pagedByMonthTransactionReader(@Qualifier("dataSource") DataSource dataSource) {
        return new JdbcPagingItemReaderBuilder<TotalMonthlyBalance>()
                .name("paged_transaction_item_reader")
                .dataSource(dataSource)
                .selectClause("SELECT date_part('month', datetime) as month, sum(amount) as balance")
                .fromClause("FROM bank.transaction")
                .groupClause("GROUP BY month")
                .sortKeys(Map.of("balance", Order.DESCENDING))
                .pageSize(10)
                .rowMapper((rs, rowNum) -> {
                    final var month = rs.getString("month");
                    final var balance = rs.getBigDecimal("balance");
                    return new TotalMonthlyBalance(month, balance);
                }).build();
    }

    @Bean
    public ItemWriter<TotalMonthlyBalance> monthTotalBalanceWriter(@Value("file:total_month_balance.json") WritableResource destination) {
        return new JsonFileItemWriterBuilder<TotalMonthlyBalance>()
                .name("json_total_month_balance_writer")
                .resource(destination)
                .jsonObjectMarshaller(new GsonJsonObjectMarshaller<>())
                .build();
    }
}
