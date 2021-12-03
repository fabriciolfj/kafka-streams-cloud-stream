package com.github.fabriciolfj.kafkastream.adapter;

import com.github.fabriciolfj.kafkastream.model.Order;
import com.github.fabriciolfj.kafkastream.model.Transaction;
import com.github.fabriciolfj.kafkastream.model.TransactionTotal;
import com.github.fabriciolfj.kafkastream.model.TransactionTotalWithProduct;
import com.github.fabriciolfj.kafkastream.usecase.OrderUseCase;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class OrderKafka {

    private static long transactionId = 0;

    private final OrderUseCase useCase;

    @Bean
    public BiConsumer<KStream<Long, Order>, KStream<Long, Order>> orders() {
        return (buy, sell) -> buy
                .merge(sell)
                .peek((k, v) -> {
                    log.info("New({}): {}", k, v);
                    useCase.add(v);
                });
    }

    @Bean
    public BiFunction<KStream<Long, Order>, KStream<Long, Order>, KStream<Long, Transaction>> transactions() {
        return (buy, sell) -> buy
                .selectKey((k, v) -> v.getProductId())
                .join(sell.selectKey((k, v) -> v.getProductId()),
                        (b, s) -> this.execute(b, s),
                        JoinWindows.of(Duration.ofSeconds(10)),
                        StreamJoined.with(Serdes.Integer(),
                                new JsonSerde<>(Order.class),
                                new JsonSerde<>(Order.class)))
                .filterNot((k, v) -> v == null)
                .map((k, v) -> new KeyValue<>(v.getId(), v))
                .peek((k, v) -> log.info("Done -> {}", v));

    }

    @Bean
    public Consumer<KStream<Long, Transaction>> total() {
        log.info("Total iniciando");
        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(
                "all-transactions-store");
        return transactions -> transactions
                .groupBy((k, v) -> {
                    log.info("Transaction: {}", v.toString());
                    return v.getStatus();
                        },
                        Grouped.with(Serdes.String(), new JsonSerde<>(Transaction.class)))
                .aggregate(
                        TransactionTotal::new,
                        (k, v, a) -> {
                            a.setCount(a.getCount() + 1);
                            a.setProductCount(a.getProductCount() + v.getAmount());
                            a.setAmount(a.getAmount() + (v.getPrice() * v.getAmount()));
                            return a;
                        },
                        Materialized.<String, TransactionTotal> as(storeSupplier)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(TransactionTotal.class)))
                .toStream()
                .peek((k, v) -> log.info("Total Teste: {}", v));
    }

    @Bean
    public BiConsumer<KStream<Long, Transaction>, KStream<Long, Order>> totalPerProduct() {
        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(
                "transactions-per-product-store");
        return (transactions, orders) -> transactions
                .selectKey((k, v) -> v.getSellOrderId())
                .join(orders.selectKey((k, v) -> v.getId()),
                        (t, o) -> new TransactionTotalWithProduct(t, o.getProductId()),
                        JoinWindows.of(Duration.ofSeconds(10)),
                        StreamJoined.with(Serdes.Long(),
                                new JsonSerde<>(Transaction.class),
                                new JsonSerde<>(Order.class)))
                .groupBy((k, v) -> v.getProductId(),
                        Grouped.with(Serdes.Integer(), new JsonSerde<>(TransactionTotalWithProduct.class)))
                .aggregate(
                        TransactionTotal::new,
                        (k, v, a) -> {
                            a.setCount(a.getCount() + 1);
                            a.setProductCount(a.getProductCount() + v.getTransaction().getAmount());
                            a.setAmount(a.getAmount() + (v.getTransaction().getPrice() * v.getTransaction().getAmount()));
                            return a;
                        },
                        Materialized.<Integer, TransactionTotal> as(storeSupplier)
                                .withKeySerde(Serdes.Integer())
                                .withValueSerde(new JsonSerde<>(TransactionTotal.class)))
                .toStream()
                .peek((k, v) -> log.info("Total per product({}): {}", k, v));
    }

    //iterar por um periodo de tempo
    @Bean
    public BiConsumer<KStream<Long, Transaction>, KStream<Long, Order>> latestPerProduct() {
        WindowBytesStoreSupplier storeSupplier = Stores.persistentWindowStore(
                "latest-transactions-per-product-store", Duration.ofSeconds(30), Duration.ofSeconds(30), false);
        return (transactions, orders) -> transactions
                .selectKey((k, v) -> v.getSellOrderId())
                .join(orders.selectKey((k, v) -> v.getId()),
                        (t, o) -> new TransactionTotalWithProduct(t, o.getProductId()),
                        JoinWindows.of(Duration.ofSeconds(10)),
                        StreamJoined.with(Serdes.Long(), new JsonSerde<>(Transaction.class), new JsonSerde<>(Order.class)))
                .groupBy((k, v) -> v.getProductId(), Grouped.with(Serdes.Integer(), new JsonSerde<>(TransactionTotalWithProduct.class)))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(30)))
                .aggregate(
                        TransactionTotal::new,
                        (k, v, a) -> {
                            a.setCount(a.getCount() + 1);
                            a.setAmount(a.getAmount() + v.getTransaction().getAmount());
                            return a;
                        },
                        Materialized.<Integer, TransactionTotal> as(storeSupplier)
                                .withKeySerde(Serdes.Integer())
                                .withValueSerde(new JsonSerde<>(TransactionTotal.class)))
                .toStream()
                .peek((k, v) -> log.info("Total per product last 30s({}): {}", k, v));
    }

    private Transaction execute(final Order buy, final Order sell) {
        if (buy.getAmount() >= sell.getAmount()) {
            int count = Math.min(buy.getProductCount(), sell.getProductCount());
            boolean allowed = useCase.performUpdate(buy.getId(), sell.getId(), count);

            if (allowed) {
                return Transaction.builder()
                        .id(++transactionId)
                        .buyOrderId(buy.getId())
                        .sellOrderId(sell.getId())
                        .amount(count)
                        .price((buy.getAmount() + sell.getAmount()) / 2)
                        .creationTime(LocalDateTime.now())
                        .status("NEW")
                        .build();
            }

            return null;
        }

        return null;
    }
}
