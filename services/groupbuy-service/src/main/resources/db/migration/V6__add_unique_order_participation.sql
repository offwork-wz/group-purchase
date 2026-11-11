-- 기존 중복 주문이 있으면 적용을 중단한다. 주문/결제 데이터는 자동 삭제하지 않는다.
ALTER TABLE tb_order
    ADD CONSTRAINT uq_order_participation UNIQUE (participation_id);
