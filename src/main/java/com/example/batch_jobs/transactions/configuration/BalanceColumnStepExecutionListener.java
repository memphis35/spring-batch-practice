package com.example.batch_jobs.transactions.configuration;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

@Component
public class BalanceColumnStepExecutionListener implements StepExecutionListener {

    private final JdbcTemplate jdbcTemplate;

    public BalanceColumnStepExecutionListener(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void beforeStep(@Nullable StepExecution stepExecution) {
        jdbcTemplate.execute("ALTER TABLE bank.transaction ADD COLUMN IF NOT EXISTS balance DECIMAL(8,2)");
    }

    @Override
    public ExitStatus afterStep(@Nullable StepExecution stepExecution) {
        return ExitStatus.COMPLETED;
    }
}
