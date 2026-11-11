package groupbuy_service.global;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ErrorCode {
    //server
    INTERNAL_ERROR(500,"서버에러"),
    INVALID_REQUEST(400, "요청 값이 올바르지 않습니다"),
    GROUPBUY_PRODUCT_MISMATCH(400, "공동구매 상품과 요청 상품이 일치하지 않습니다"),

    //user
    USER_NOT_FOUND(404,"사용자를 찾을 수 없습니다"),
    EMAIL_ALREADY_EXISTS(400,"동일한 이메일의 사용자가 이미 존재합니다"),

    //product, groupbuy, participation
    PRODUCT_NAME_ALREADY_EXISTS(400,"동일한 이름의 상품이 이미 존재합니다"),
    PRODUCT_NOT_FOUND(404,"상품을 찾을수 없습니다"),
    PRODUCT_OUT_OF_STOCK(400,"재고가 없습니다."),
    PARTICIPATION_DATE_EXPIRED(400,"참여 기간이 아닙니다"),
    PARTICIPATION_NOT_AVAILABLE(400,"현재 참여 가능한 상태가 아닙니다."),
    ORDER_QUANTITY_INVALID(400,"주문 수량이 올바르지 않습니다"),
    GROUPBUY_NOT_FOUND(404,"공동구매 정보가 없습니다"),
    GROUPBUY_STARTAT_INVALID(400,"시작일이 올바르지 않습니다"),
    GROUPBUY_ENDAT_INVALID(400,"종료일이 올바르지 않습니다"),
    GROUPBUY_INVALID(400,"참여할 수 없는 공동구매 입니다"),
    INITIAL_STOCK_INVALID(400, "초기 재고는 0보다 작을 수 없습니다"),
    PRODUCT_PRICE_INVALID(400, "상품 가격은 0보다 작을 수 없습니다"),
    TARGET_QUANTITY_INVALID(400, "목표 수량이 올바르지 않습니다"),
    ORDER_NOT_FOUND(404, "주문 정보가 없습니다"),
    ;

    private final int status;
    private final String msg;
}
