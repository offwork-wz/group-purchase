package groupbuy_service.groupbuy.service;

import groupbuy_service.groupbuy.domain.Groupbuy;
import groupbuy_service.groupbuy.dto.GroupbuyResponse;
import java.time.LocalDateTime;

public interface GroupbuyService {
    GroupbuyResponse getGroupbuy(String groupbuyId);
    Groupbuy createGroupbuy(String productId, int targetQuantity, LocalDateTime startAt, LocalDateTime endAt);
    void openGroupbuy(String groupbuyId);
}
