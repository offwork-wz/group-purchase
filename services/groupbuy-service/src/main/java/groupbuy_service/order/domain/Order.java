package groupbuy_service.order.domain;

import groupbuy_service.common.BaseEntity;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.UUID;

@Entity
@Table(name = "tb_order")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Order extends BaseEntity {

    @Id
    private String orderId;

    private String groupbuyId;
    private String productId;
    private String userId;
    @Column(nullable = false, unique = true)
    private String participationId;

    private int quantity;
    @Column(precision = 15, scale = 2)
    private BigDecimal totalPrice;
    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    @Builder
    public Order (String groupbuyId,
                  String productId,
                  String userId,
                  String participationId,
                  int quantity,
                  BigDecimal totalPrice
    ) {
        this.orderId = UUID.randomUUID().toString();
        this.status = OrderStatus.CREATED;

        this.groupbuyId = groupbuyId;
        this.productId = productId;
        this.userId = userId;
        this.participationId = participationId;
        this.quantity = quantity;
        this.totalPrice = totalPrice;
    }

    public void updateStatus(OrderStatus status) {
        this.status = status;
    }

}
