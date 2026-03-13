package org.javaup.utils;

import org.javaup.dto.UserDTO;

/**
 * @description: 用户持有器-黑马点评普通版本和plus版本使用
 */
public class UserHolder {
    private static final ThreadLocal<UserDTO> TL = new ThreadLocal<>();

    public static void saveUser(UserDTO user){
        TL.set(user);
    }

    public static UserDTO getUser(){
        return TL.get();
    }

    public static void removeUser(){
        TL.remove();
    }
}
