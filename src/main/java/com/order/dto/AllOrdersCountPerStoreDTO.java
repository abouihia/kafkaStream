package com.order.dto;

import com.order.domain.OrderType;

public record AllOrdersCountPerStoreDTO(String locationId,
                                        Long orderCount,
                                        OrderType orderType) {
}
