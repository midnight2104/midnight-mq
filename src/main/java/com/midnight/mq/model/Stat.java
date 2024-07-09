package com.midnight.mq.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Stat {
    private Subscription subscription;
    private int total;
    private int position;

}
