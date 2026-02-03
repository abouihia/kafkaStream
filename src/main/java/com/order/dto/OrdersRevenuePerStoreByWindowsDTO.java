package com.order.dto;

import com.order.domain.OrderType;
import com.order.domain.TotalRevenue;
import java.time.LocalDateTime;

public record OrdersRevenuePerStoreByWindowsDTO(String locationId,
                                                TotalRevenue totalRevenue,
                                                OrderType orderType,
                                                LocalDateTime startWindow,
                                                LocalDateTime endWindow) {
}
