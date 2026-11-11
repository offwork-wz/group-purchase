
package groupbuy_service;

import groupbuy_service.groupbuy.domain.Groupbuy;
import groupbuy_service.groupbuy.dto.GroupbuyCreateRequest;
import groupbuy_service.groupbuy.dto.GroupbuyResponse;
import groupbuy_service.groupbuy.service.GroupbuyService;
import jakarta.validation.Valid;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;

@RestController
@RequestMapping("/groupbuy")
@RequiredArgsConstructor
public class GroupbuyController {

    private final GroupbuyService groupbuyService;

    @GetMapping("/{groupbuyId}")
    public GroupbuyResponse getGroupbuy(@PathVariable String groupbuyId) {
        return groupbuyService.getGroupbuy(groupbuyId);
    }

    @PostMapping
    public String createGroupbuy(@Valid @RequestBody GroupbuyCreateRequest request) {
        Groupbuy groupbuy = groupbuyService.createGroupbuy(
                request.getProductId(),
                request.getTargetQuantity(),
                request.getStartAt(),
                request.getEndAt()
        );
        return "공동구매가 등록되었습니다. (ID: " + groupbuy.getGroupbuyId() + ")";
    }

    @PostMapping("/{groupbuyId}/open")
    public String openGroupbuy(@PathVariable String groupbuyId) {
        groupbuyService.openGroupbuy(groupbuyId);
        return "공동구매가 활성화(OPEN) 되었습니다.";
    }

}
