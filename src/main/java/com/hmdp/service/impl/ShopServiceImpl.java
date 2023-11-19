package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    public CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //解决缓存穿透
        Shop shop = cacheClient.queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //解决缓存击穿 用互斥锁
//        Shop shop = queryWithMutex(id);
        //再热点key背景下，逻辑过期解决缓存击穿问题
//        Shop shop = cacheClient.queryWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    //避免缓存穿透
    private Shop queryWithPassThrough(Long id) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        //1.从redis中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StringUtils.hasText(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            //3.存在直接返回
            return shop;
        }
        //判断命中的是否是空值
        if (shopJson != null) {
            //返回一个错误信息
            return null;
        }

        //4.实现缓存重建
        //4.1 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY+id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //4.2 判断是否获取成功
            if (!isLock) {
                //4.3 失败 则休眠并重试
                Thread.sleep(50);
                return queryWithPassThrough(id);
            }

            //根据id查数据库

            shop = getById(id);
            //若查到的为空值
            if (shop == null) {
                //将空值写入redis 防止缓存穿透
                stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                //5.不存在 返回错误
                return null;
            }
            //6.存在 写入redis
            //shop转为Json
            String jsonStr = JSONUtil.toJsonStr(shop);
            stringRedisTemplate.opsForValue().set(key, jsonStr, 30, TimeUnit.MINUTES);

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //释放互斥锁
            unLock(lockKey);
        }
        return shop;
    }

    //用互斥锁避免缓存击穿
    private Shop queryWithMutex(Long id) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        //1.从redis中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StringUtils.hasText(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            //3.存在直接返回
            return shop;
        }
        //判断命中的是否是空值
        if (shopJson != null) {
            //返回一个错误信息
            return null;
        }

        //4.实现缓存重建
        //4.1 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY+id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //4.2 判断是否获取成功
            if (!isLock) {
                //4.3 失败 则休眠并重试
                Thread.sleep(50);
                return queryWithPassThrough(id);
            }

            //根据id查数据库

            shop = getById(id);
            //若查到的为空值
            if (shop == null) {
                //将空值写入redis 防止缓存穿透
                stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                //5.不存在 返回错误
                return null;
            }
            //6.存在 写入redis
            //shop转为Json
            String jsonStr = JSONUtil.toJsonStr(shop);
            stringRedisTemplate.opsForValue().set(key, jsonStr, 30, TimeUnit.MINUTES);

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //释放互斥锁
            unLock(lockKey);
        }
        return shop;
    }

    //创建线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    //使用逻辑过期避免缓存击穿
    //这里的背景是在热点key的情况下，事先把热点key存到了redis中。不需要考虑缓存穿透
    private Shop queryWithLogicalExpire(Long id) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        //1.从redis中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (!StringUtils.hasText(shopJson)) {
            //3.不存在直接返回
            return null;
        }
        //4.命中，先反序列化
        //字符串shopJson转为RedisData，此时data实际为JSONObject对象
        //data再转为Shop对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //5.1未过期，返回店铺信息
            return shop;
        }
        //5.2过期，需要缓存重建

        //6.缓存重建
        //6.1获取互斥锁
        String locakKey = RedisConstants.LOCK_SHOP_KEY+id;
        boolean isLock = tryLock(locakKey);
        //6.2判断是否获取成功
        if (isLock) {
            //6.3成功 开启独立线程，实现缓存重建
            //这里要做双重检查double check
            //这里获取到的锁可能是上一个线程刚刚做完缓存重建所释放的，这样的话目标数据已经缓存重建完毕了，因此有必要做双重检查
            CACHE_REBUILD_EXECUTOR.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        //重建缓存
                        saveShop2Redis(id, 30L);
                        Thread.sleep(200);  //模拟延迟
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        //释放锁 要写到finally中
                        unLock(locakKey);
                    }
                }
            });
        }
        //6.4返回过期的商铺信息
        return shop;
    }

    @Override
    @Transactional   //分布式系统中得用其他方案 单体项目中用事务即可
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不存在");
        }
        //1.先更新数据库
        updateById(shop);
        //2.后删除缓存
        String key = RedisConstants.CACHE_SHOP_KEY+id;
        stringRedisTemplate.delete(key);
        return Result.ok();
    }

    //获取锁
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        //直接用装箱后的包装类返回flag，可能会导致空指针异常，这里处理一下
        return BooleanUtil.isTrue(flag);
    }

    //释放锁
    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }

    //加上逻辑过期后的把数据存储到Redis
    public void saveShop2Redis(Long id, long expireTime) {
        //查询店铺数据
        Shop shop = getById(id);
        //封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireTime));
        //写入Redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id, JSONUtil.toJsonStr(redisData));
    }
}
