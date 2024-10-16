package com.example.batch_jobs.transactions.configuration;

import com.example.batch_jobs.transactions.domain.MerchantTransaction;
import com.example.batch_jobs.transactions.domain.TotalBalance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Optional;

@Component
public class SequentialBalanceProcessor implements ItemProcessor<MerchantTransaction, TotalBalance> {

    private ExecutionContext context;

    @Override
    public TotalBalance process(@NonNull MerchantTransaction item) {
        final BigDecimal updatedBalance = Optional.ofNullable(context.get("currentBalance", BigDecimal.class))
                .orElse(BigDecimal.ZERO)
                .add(item.amount());
        context.put("currentBalance", updatedBalance);
        return new TotalBalance(item.id(), updatedBalance);
    }

    public void setStepExecution(@Nullable StepExecution stepExecution) {
        this.context = Optional.ofNullable(stepExecution)
                .map(StepExecution::getExecutionContext)
                .orElse(null);
    }
}
