package groupbuy_service.groupbuy.service;

import groupbuy_service.global.BusinessException;
import groupbuy_service.global.ErrorCode;
import groupbuy_service.groupbuy.domain.Groupbuy;
import groupbuy_service.groupbuy.domain.GroupbuyStatus;
import groupbuy_service.groupbuy.repository.GroupbuyRepository;
import groupbuy_service.groupbuy.dto.GroupbuyResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class GroupbuyServiceImpl implements GroupbuyService {

    private final GroupbuyRepository groupbuyRepository;

    @Override
    @Transactional(readOnly = true)
    public GroupbuyResponse getGroupbuy(String groupbuyId) {
        return GroupbuyResponse.from(groupbuyRepository.findById(groupbuyId)
                .orElseThrow(() -> new BusinessException(ErrorCode.GROUPBUY_NOT_FOUND)));
    }

    @Override
    @Transactional
    public Groupbuy createGroupbuy(String productId, int targetQuantity, LocalDateTime startAt, LocalDateTime endAt) {
        Groupbuy groupbuy = Groupbuy.builder()
                .productId(productId)
                .targetQuantity(targetQuantity)
                .startAt(startAt)
                .endAt(endAt)
                .build();
        
        Groupbuy saved = groupbuyRepository.save(groupbuy);
        log.info("[GroupbuyService] 공동구매 등록 완료: id={}", saved.getGroupbuyId());
        return saved;
    }

    @Override
    @Transactional
    public void openGroupbuy(String groupbuyId) {
        Groupbuy groupbuy = groupbuyRepository.findById(groupbuyId)
                .orElseThrow(() -> new BusinessException(ErrorCode.GROUPBUY_NOT_FOUND));
        
        if (groupbuy.getStatus() != GroupbuyStatus.CLOSED) {
            log.warn("[GroupbuyService] 이미 오픈되었거나 완료된 공동구매입니다. id={}, status={}", groupbuyId, groupbuy.getStatus());
            throw new BusinessException(ErrorCode.GROUPBUY_INVALID);
        }
        
        groupbuy.updateStatus(GroupbuyStatus.OPEN);
        log.info("[GroupbuyService] 공동구매 오픈 활성화 성공: id={}", groupbuyId);
    }
}
