package groupbuy_service.order.service;

import groupbuy_service.global.BusinessException;
import groupbuy_service.global.ErrorCode;
import groupbuy_service.groupbuy.repository.GroupbuyRepository;
import groupbuy_service.groupbuy.domain.Groupbuy;
import groupbuy_service.groupbuy.domain.GroupbuyStatus;
import groupbuy_service.order.domain.Order;
import groupbuy_service.order.domain.OrderStatus;
import groupbuy_service.order.event.OrderCancelledEvent;
import groupbuy_service.order.event.OrderCreatedEvent;
import groupbuy_service.order.event.OrderInfo;
import groupbuy_service.order.repository.OrderRepository;
import groupbuy_service.participation.domain.Participation;
import groupbuy_service.participation.domain.ParticipationStatus;
import groupbuy_service.participation.repository.ParticipationRepository;
import groupbuy_service.product.domain.Product;
import groupbuy_service.product.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService{

    private final OrderRepository orderRepository;
    private final ParticipationRepository participationRepository;
    private final GroupbuyRepository groupbuyRepository;
    private final ProductRepository productRepository;
    private final ApplicationEventPublisher eventPublisher;

    @Override
    @Transactional
    public void createOrdersForGroupbuy(String groupbuyId, String productId) {
        Groupbuy groupbuy = groupbuyRepository.findByIdWithLock(groupbuyId)
                .orElseThrow(() -> new BusinessException(ErrorCode.GROUPBUY_NOT_FOUND));
        if (!groupbuy.getProductId().equals(productId)) {
            throw new BusinessException(ErrorCode.GROUPBUY_PRODUCT_MISMATCH);
        }
        if (groupbuy.getStatus() != GroupbuyStatus.COMPLETED) {
            throw new BusinessException(ErrorCode.GROUPBUY_INVALID);
        }

        List<Participation> participations = participationRepository.findWithoutOrder(groupbuyId, ParticipationStatus.SUCCESS);
        if (participations.isEmpty()){
            log.info("추가로 주문할 참여가 없습니다. groupbuyId: {}", groupbuyId);
            return;
        }

        Product product = productRepository.findById(productId).orElseThrow(
                () -> new BusinessException(ErrorCode.PRODUCT_NOT_FOUND));

        List<Order> orders = participations.stream().map(
                p -> {
                    BigDecimal totalPrice = product.getPrice().multiply(BigDecimal.valueOf(p.getQuantity()));
                    return Order.builder()
                            .groupbuyId(groupbuyId)
                            .productId(productId)
                            .userId(p.getUserId())
                            .participationId(p.getParticipationId())
                            .quantity(p.getQuantity())
                            .totalPrice(totalPrice)
                            .build();
                }).toList();

        orderRepository.saveAll(orders);
        log.info("공동구매 주문 일괄 생성 groupbuyId: {}, order: {}건", groupbuyId, orders.size());

        List<OrderInfo> list = orders.stream().map(o ->
            new OrderInfo(o.getOrderId(), o.getUserId(), o.getTotalPrice())
        ).toList();

        OrderCreatedEvent event = OrderCreatedEvent.of(groupbuyId, list);
        eventPublisher.publishEvent(event);

    }

    @Override
    @Transactional
    public void cancelOrder(String orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new BusinessException(ErrorCode.ORDER_NOT_FOUND));

        order.updateStatus(OrderStatus.CANCELLED);
        log.info("주문 취소 처리 성공: orderId{}", orderId);

        OrderCancelledEvent event = OrderCancelledEvent.of(
                order.getParticipationId()
                , orderId
                , order.getProductId()
                , order.getQuantity());
        eventPublisher.publishEvent(event);
    }

    @Override
    @Transactional
    public void completedOrder(String orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new BusinessException(ErrorCode.ORDER_NOT_FOUND));

        order.updateStatus(OrderStatus.PAID);
        log.info("주문 결제 성공: orderId{}", orderId);
    }
}
