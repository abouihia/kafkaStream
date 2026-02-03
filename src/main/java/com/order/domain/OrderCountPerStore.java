package com.order.domain;

public record OrderCountPerStore(String locationId,
                                 Long orderCount) {
}
