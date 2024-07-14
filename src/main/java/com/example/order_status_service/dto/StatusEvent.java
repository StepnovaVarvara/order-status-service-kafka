package com.example.order_status_service.dto;

import lombok.Data;
import lombok.experimental.Accessors;

import java.time.Instant;

@Data
@Accessors(chain = true)
public class StatusEvent {
    private String status;
    private Instant date;
}
