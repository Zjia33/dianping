package org.javaup.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.javaup.entity.Voucher;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @description: 优惠券 Mapper
 */
public interface VoucherMapper extends BaseMapper<Voucher> {

    List<Voucher> queryVoucherOfShop(@Param("shopId") Long shopId);
}
