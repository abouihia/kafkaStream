package com.order.domain;

public record AllOrdersCountPerStore(String locationId,
                                     Long orderCount,
                                     OrderType orderType) {
}
