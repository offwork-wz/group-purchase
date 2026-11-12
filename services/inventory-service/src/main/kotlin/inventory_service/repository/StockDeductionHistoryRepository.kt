package inventory_service.repository

import inventory_service.domain.StockDeductionHistory
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param

interface StockDeductionHistoryRepository : JpaRepository<StockDeductionHistory, String> {
    @Modifying
    @Query(
        value = """
            INSERT INTO stock_deduction_history (
                participation_id, groupbuy_id, product_id, user_id, quantity, status, result_event_id
            ) VALUES (
                :participationId, :groupbuyId, :productId, :userId, :quantity, 'PROCESSING', :resultEventId
            )
            ON CONFLICT (participation_id) DO NOTHING
        """,
        nativeQuery = true
    )
    fun insertIgnoringDuplicate(
        @Param("participationId") participationId: String,
        @Param("groupbuyId") groupbuyId: String,
        @Param("productId") productId: String,
        @Param("userId") userId: String,
        @Param("quantity") quantity: Int,
        @Param("resultEventId") resultEventId: String
    ): Int
}
