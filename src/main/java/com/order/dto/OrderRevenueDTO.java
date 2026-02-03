package com.order.dto;

import com.order.domain.OrderType;
import com.order.domain.TotalRevenue;

public record OrderRevenueDTO (String locationId,
                               OrderType orderType,
                               TotalRevenue totalRevenue){
}
