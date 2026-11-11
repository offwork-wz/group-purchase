package inventory_service.repository

import inventory_service.domain.StockRestoreHistory
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param

interface StockRestoreHistoryRepository : JpaRepository<StockRestoreHistory, String> {
    @Modifying
    @Query(
        value = """
            INSERT INTO stock_restore_history (order_id, product_id, quantity)
            VALUES (:orderId, :productId, :quantity)
            ON CONFLICT (order_id) DO NOTHING
        """,
        nativeQuery = true
    )
    fun insertIgnoringDuplicate(
        @Param("orderId") orderId: String,
        @Param("productId") productId: String,
        @Param("quantity") quantity: Int
    ): Int
}
