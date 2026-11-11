package groupbuy_service.participation.repository;

import groupbuy_service.participation.domain.Participation;
import groupbuy_service.participation.domain.ParticipationStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;

@Repository
public interface ParticipationRepository extends JpaRepository<Participation, String> {
    List<Participation> findAllByGroupbuyIdAndStatus(String groupbuyId, ParticipationStatus status);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select p from Participation p where p.participationId = :participationId")
    Optional<Participation> findByIdWithLock(@Param("participationId") String participationId);

    @Query("""
            select p from Participation p
            where p.groupbuyId = :groupbuyId and p.status = :status
              and not exists (select o.orderId from Order o where o.participationId = p.participationId)
            """)
    List<Participation> findWithoutOrder(@Param("groupbuyId") String groupbuyId,
                                         @Param("status") ParticipationStatus status);
}
