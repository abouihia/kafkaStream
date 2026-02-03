package com.withoutspring.domain;

import java.math.BigDecimal;

public record TotalRevenue(String locationId,
                           Integer runningOrderItem,
                           BigDecimal runningRevenue) {


    public TotalRevenue() {
        this("",0, BigDecimal.valueOf(0.0));
    }

   public TotalRevenue  updateTotalRevenue(String key, Order orderFormStream){

       var newRunningOrderCount = this.runningOrderItem +1;
       BigDecimal  revenue = this.runningRevenue.add(orderFormStream.finalAmount());
       return new TotalRevenue(key, newRunningOrderCount, revenue);
    }
}
