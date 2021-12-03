package com.github.fabriciolfj.kafkastream.config;

import com.github.fabriciolfj.kafkastream.model.Order;
import com.github.fabriciolfj.kafkastream.model.OrderType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.function.Supplier;

@Configuration
@Slf4j
public class OrderConfiguration {

    private static long orderId = 100;
    private static final Random r = new Random();

    LinkedList<Order> buyOrders = new LinkedList<>(List.of(
            new Order(++orderId, 1, 1, 100, LocalDateTime.now(), OrderType.BUY, 1000),
            new Order(++orderId, 2, 1, 200, LocalDateTime.now(), OrderType.BUY, 1050),
            new Order(++orderId, 3, 1, 100, LocalDateTime.now(), OrderType.BUY, 1030),
            new Order(++orderId, 4, 1, 200, LocalDateTime.now(), OrderType.BUY, 1050),
            new Order(++orderId, 5, 2, 200, LocalDateTime.now(), OrderType.BUY, 1000),
            new Order(++orderId, 11, 2, 100, LocalDateTime.now(), OrderType.BUY, 1050)
    ));

    LinkedList<Order> sellOrders = new LinkedList<>(List.of(
            new Order(++orderId, 6, 1, 200, LocalDateTime.now(), OrderType.SELL, 950),
            new Order(++orderId, 7, 1, 100, LocalDateTime.now(), OrderType.SELL, 1000),
            new Order(++orderId, 8, 1, 100, LocalDateTime.now(), OrderType.SELL, 1050),
            new Order(++orderId, 9, 1, 300, LocalDateTime.now(), OrderType.SELL, 1000),
            new Order(++orderId, 10, 1, 200, LocalDateTime.now(), OrderType.SELL, 1020)
    ));

    @Bean
    public Supplier<Message<Order>> orderBuy() {
        return () -> {
            if (buyOrders.peek() != null) {
                Message<Order> o = MessageBuilder
                        .withPayload(buyOrders.peek())
                        .setHeader(KafkaHeaders.MESSAGE_KEY, Objects.requireNonNull(buyOrders.poll()).getId())
                        .build();
                log.info("Order: buy {}", o.getPayload());
                return o;
            } else {
                return null;
            }
        };
    }

    @Bean
    public Supplier<Message<Order>> orderSell() {
        return () -> {
            if (sellOrders.peek() != null) {
                Message<Order> o = MessageBuilder
                        .withPayload(sellOrders.peek())
                        .setHeader(KafkaHeaders.MESSAGE_KEY, Objects.requireNonNull(sellOrders.poll()).getId())
                        .build();
                log.info("Order sell: {}", o.getPayload());
                return o;
            } else {
                return null;
            }
        };
    }

}
