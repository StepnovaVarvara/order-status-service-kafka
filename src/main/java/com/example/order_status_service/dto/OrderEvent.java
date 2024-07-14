package com.example.order_status_service.dto;

import lombok.Data;

@Data
public class OrderEvent {
    private String product;
    private Integer quantity;
}
