package com.hmdp.dto;

import lombok.Data;

import java.util.List;

@Data
public class ScrollResult {
    /**
     * 滚动分页
     */
    private List<?> list;
    /**
     * 最小时间
     */
    private Long minTime;
    /**
     * 偏移量
     */
    private Integer offset;
}
