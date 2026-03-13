package org.javaup.dto;

import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serial;
import java.io.Serializable;

/**
 * @description: 延迟优惠券提醒
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class DelayVoucherReminderDto implements Serializable {
    
    @Serial
    private static final long serialVersionUID = 1L;
    
    /**
     * 优惠券id
     * */
    @NotNull
    private Long voucherId;
    
    /**
     * 延迟时间，单位秒
     * */
    @NotNull
    private Integer delaySeconds;
}
