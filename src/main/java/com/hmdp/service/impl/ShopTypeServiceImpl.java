package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate stringRedisTemplates;

    @Override
    public Result queryShopType() {
        String key = RedisConstants.SHOP_TYPE_KEY+"types";
        //在缓存中找
        String types = stringRedisTemplates.opsForValue().get(key);
        if (StringUtils.hasText(types)) {
            List<ShopType> list = JSONUtil.toList(types, ShopType.class);
            return Result.ok(list);
        }
        //在mysql中找
        List<ShopType> shopTypes = query().orderByAsc("sort").list();
        if (shopTypes == null) {
            return Result.fail("无店铺种类信息");
        }


        //缓存到redis中

        //用string存
        stringRedisTemplates.opsForValue().set(key, JSONUtil.toJsonStr(shopTypes));
        return Result.ok(shopTypes);

        //用list存
//        for (ShopType shopType : shopTypes) {
//            stringRedisTemplates.opsForList().rightPush(key, JSONUtil.toJsonStr(shopType));
//        }
//        return Result.ok(shopTypes);


        //csdn的

//        // 1.先去Redis查缓存首页缓存数据
//        String shopTypeListJson = stringRedisTemplates.opsForValue().get(RedisConstants.SHOP_TYPE_KEY);
//
//        // 2.如果为不为空，则直接返回
//        if (StrUtil.isNotBlank(shopTypeListJson)) {
//            return Result.ok(JSONUtil.toList(shopTypeListJson,ShopType.class));
//        }
//
//        // 3.如果为空，去查询数据库
//        List <ShopType> shopTypeList = this.query().orderByDesc("sort").list();
//
//        // 4.如果查询为空，则直接返回错误信息
//        if (shopTypeList == null || shopTypeList.size() == 0) {
//            return Result.fail("商品类型查询失败！");
//        }
//
//        // 5.如果不为空，则把数据存入到Redis,并返回结果 (这里可以使用List,String等结构)
//        stringRedisTemplates.opsForValue().set(RedisConstants.SHOP_TYPE_KEY,JSONUtil.toJsonStr(shopTypeList));
//        return Result.ok(shopTypeList);

    }
}
