package groupbuy_service;

import groupbuy_service.participation.service.ParticipationService;
import groupbuy_service.participation.domain.ParticipationRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/groupbuy")
@RequiredArgsConstructor
public class ParticipationController {

    private final ParticipationService participationService;

    @PostMapping("/{groupbuyId}/participate")
    public String participate(
            @PathVariable String groupbuyId,
            @Valid @RequestBody ParticipationRequest request
    ) {
        participationService.participate(groupbuyId, request);
        return "참여 요청이 접수되었습니다. (groupbuyId: " + groupbuyId + ")";
    }



}
