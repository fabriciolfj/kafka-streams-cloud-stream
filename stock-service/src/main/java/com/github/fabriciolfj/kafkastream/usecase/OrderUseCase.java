package com.github.fabriciolfj.kafkastream.usecase;

import com.github.fabriciolfj.kafkastream.model.Order;
import com.github.fabriciolfj.kafkastream.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class OrderUseCase {

    private final OrderRepository repository;

    public Order add(Order order) {
        return repository.save(order);
    }

    @Transactional
    public boolean performUpdate(Long buyOrderId, Long sellOrderId, int amount) {
        Order buyOrder = repository.findById(buyOrderId).orElseThrow();
        Order sellOrder = repository.findById(sellOrderId).orElseThrow();
        int buyAvailableCount = buyOrder.getProductCount() - buyOrder.getRealizedCount();
        int sellAvailableCount = sellOrder.getProductCount() - sellOrder.getRealizedCount();
        if (buyAvailableCount >= amount && sellAvailableCount >= amount) {
            buyOrder.setRealizedCount(buyOrder.getRealizedCount() + amount);
            sellOrder.setRealizedCount(sellOrder.getRealizedCount() + amount);
            repository.save(buyOrder);
            repository.save(sellOrder);
            return true;
        } else {
            return false;
        }
    }
}
