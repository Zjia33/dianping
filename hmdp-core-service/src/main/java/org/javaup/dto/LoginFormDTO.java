package org.javaup.dto;

import lombok.Data;

/**
 * @description: 登录-入参
 */
@Data
public class LoginFormDTO {
    private String phone;
    private String code;
    private String password;
}
