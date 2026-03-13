package org.javaup.service.impl;

import org.javaup.entity.ShopType;
import org.javaup.mapper.ShopTypeMapper;
import org.javaup.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

/**
 * @description: 商铺类型 接口实现
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

}
