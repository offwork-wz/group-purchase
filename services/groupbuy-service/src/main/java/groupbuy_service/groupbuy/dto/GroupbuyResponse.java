package groupbuy_service.groupbuy.dto;

import groupbuy_service.groupbuy.domain.Groupbuy;
import groupbuy_service.groupbuy.domain.GroupbuyStatus;
import java.time.LocalDateTime;

public record GroupbuyResponse(
        String groupbuyId,
        String productId,
        int targetQuantity,
        int currentQuantity,
        LocalDateTime startAt,
        LocalDateTime endAt,
        GroupbuyStatus status
) {
    public static GroupbuyResponse from(Groupbuy groupbuy) {
        return new GroupbuyResponse(groupbuy.getGroupbuyId(), groupbuy.getProductId(),
                groupbuy.getTargetQuantity(), groupbuy.getCurrentQuantity(),
                groupbuy.getStartAt(), groupbuy.getEndAt(), groupbuy.getStatus());
    }
}
