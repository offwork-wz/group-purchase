package groupbuy_service.participation.service;

import groupbuy_service.global.BusinessException;
import groupbuy_service.global.ErrorCode;
import groupbuy_service.groupbuy.domain.Groupbuy;
import groupbuy_service.groupbuy.domain.GroupbuyStatus;
import groupbuy_service.groupbuy.event.GroupbuyConfirmedEvent;
import groupbuy_service.groupbuy.repository.GroupbuyRepository;
import groupbuy_service.participation.domain.Participation;
import groupbuy_service.participation.domain.ParticipationRequest;
import groupbuy_service.participation.domain.ParticipationStatus;
import groupbuy_service.participation.event.ParticipationRequestedEvent;
import groupbuy_service.participation.repository.ParticipationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class ParticipationServiceImpl implements ParticipationService {

    private final GroupbuyRepository groupbuyRepository;
    private final ParticipationRepository participationRepository;
    private final ApplicationEventPublisher eventPublisher;

    @Transactional
    @Override
    public void participate(String groupbuyId, ParticipationRequest request) {
        Groupbuy groupbuy = groupbuyRepository.findByIdWithLock(groupbuyId)
                .orElseThrow(() -> new BusinessException(ErrorCode.GROUPBUY_NOT_FOUND));

        if (!groupbuy.getProductId().equals(request.productId())) {
            throw new BusinessException(ErrorCode.GROUPBUY_PRODUCT_MISMATCH);
        }
        if (request.quantity() <= 0) {
            throw new BusinessException(ErrorCode.ORDER_QUANTITY_INVALID);
        }

        if (groupbuy.getStatus() != GroupbuyStatus.OPEN) {
            throw new BusinessException(ErrorCode.PARTICIPATION_NOT_AVAILABLE);
        }

        Participation participation = Participation.builder()
                .groupbuyId(groupbuyId)
                .productId(request.productId())
                .userId(request.userId())
                .quantity(request.quantity())
                .build();
        
        Participation savedParticipation = participationRepository.save(participation);

        eventPublisher.publishEvent(ParticipationRequestedEvent.from(savedParticipation));
        log.info("참여 요청 이벤트 발행 예약(커밋 후 전송): participationId={}", savedParticipation.getParticipationId());
    }

    @Transactional
    @Override
    public void confirmParticipation(String participationId) {
        Participation participation = participationRepository.findByIdWithLock(participationId)
                .orElseThrow(() -> new BusinessException(ErrorCode.INTERNAL_ERROR));

        if (participation.getStatus() != ParticipationStatus.REQUESTED) {
            log.info("이미 처리된 참여 요청입니다. participationId={}, status={}", participationId, participation.getStatus());
            return;
        }

        Groupbuy groupbuy = groupbuyRepository.findByIdWithLock(participation.getGroupbuyId())
                .orElseThrow(() -> new BusinessException(ErrorCode.GROUPBUY_NOT_FOUND));

        // 완료 전에 접수된 REQUESTED 참여는 목표 달성 후에도 확정한다.
        if (groupbuy.getStatus() != GroupbuyStatus.OPEN && groupbuy.getStatus() != GroupbuyStatus.COMPLETED) {
            log.warn("공동구매가 오픈 상태가 아니므로 참여를 확정할 수 없습니다. groupbuyId={}, status={}", groupbuy.getGroupbuyId(), groupbuy.getStatus());
            participation.updateStatus(ParticipationStatus.FAILED);
            return;
        }

        groupbuy.addCurrentQuantity(participation.getQuantity());
        participation.updateStatus(ParticipationStatus.SUCCESS);
        log.info("참여 확정 성공: participationId={}", participationId);

        if (groupbuy.getStatus() == GroupbuyStatus.COMPLETED) {
            eventPublisher.publishEvent(GroupbuyConfirmedEvent.of(groupbuy.getGroupbuyId(), groupbuy.getProductId()));
            log.info("공동구매 확정 이벤트 발행 예약(커밋 후 전송): groupbuyId={}", groupbuy.getGroupbuyId());
        }

    }

    @Transactional
    @Override
    public void failParticipation(String participationId) {
        Participation participation = participationRepository.findById(participationId)
                .orElseThrow(() -> new BusinessException(ErrorCode.INTERNAL_ERROR));
        
        participation.updateStatus(ParticipationStatus.FAILED);
        log.info("참여 실패 처리 완료: participationId={}", participationId);
    }
}
