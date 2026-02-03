package com.order.domain;

import java.time.LocalDateTime;

public record AllOrdersCountPerStoreByWindows(String locationId,
                                              Long orderCount,
                                              OrderType orderType,
                                              LocalDateTime startWindow,
                                              LocalDateTime endWindow) {
}
