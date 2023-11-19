package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;


@Slf4j
@Component
public class CacheClient {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    //带过期时间的存储
    public void set(String key, Object value, long time, TimeUnit unit) {
        String jsonValue = JSONUtil.toJsonStr(value);
        stringRedisTemplate.opsForValue().set(key, jsonValue, time, unit);
    }

    //带逻辑过期的存储
    public void setWithLogicalExpire(String key, Object value, long time, TimeUnit unit) {
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(time));
        //写入Reis
        String jsonValue = JSONUtil.toJsonStr(redisData);
        stringRedisTemplate.opsForValue().set(key, jsonValue);

    }


    //使用存储空值避免缓存穿透
    //最后两个时间的参数，表示缓存时间.
    public <R, ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback,
            long time, TimeUnit unit) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        //1.从redis中查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StringUtils.hasText(json)) {
            //3.存在直接返回
            return JSONUtil.toBean(json, type);
        }
        //判断命中的是否为空值
        if (json != null) {
            //此时json为""
            return null;
        }
        //4.不存在，根据id查数据库。
        //此时不知道要在什么数据库上查，需要调用者自己写这段方法
        //T为参数，R为返回值
        R r = dbFallback.apply(id);
        //5.不存在，返回空值
        if (r == null) {
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.SECONDS);
            return null;
        }
        //6.存在,写入redis
//        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(r), time, unit);
        this.set(key, JSONUtil.toJsonStr(r), time, unit);

        return r;
    }


    //在查询时，为了避免缓存击穿，用逻辑删除
    //创建线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    //使用逻辑过期避免缓存击穿
    //这里的背景是在热点key的情况下，事先把热点key存到了redis中。不需要考虑缓存穿透
    public  <R, T> R queryWithLogicalExpire(
            String keyPrefix, T id, Class<R> type, Function<T, R> dbFallback,
            long time, TimeUnit unit) {
        String key = keyPrefix + id;
        //1.从redis中查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (!StringUtils.hasText(json)) {
            //3.不存在直接返回
            return null;
        }
        //4.命中，先反序列化
        //字符串shopJson转为RedisData，此时data实际为JSONObject对象
        //data再转为Shop对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //5.1未过期，返回店铺信息
            return r;
        }
        //5.2过期，需要缓存重建

        //6.缓存重建
        //6.1获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY+id;
        boolean isLock = tryLock(lockKey);
        //6.2判断是否获取成功
        if (isLock) {
            //6.3成功 开启独立线程，实现缓存重建
            //这里要做双重检查double check
            //这里获取到的锁可能是上一个线程刚刚做完缓存重建所释放的，这样的话目标数据已经缓存重建完毕了，因此有必要做双重检查
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //重建缓存
                    //查数据库 需要用户传入函数
                    R r1 = dbFallback.apply(id);
                    //写入redis
                    this.setWithLogicalExpire(key, r1, time, unit);
                    Thread.sleep(200);  //模拟延迟
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁 要写到finally中
                    unLock(lockKey);
                }
            });
        }
        //6.4返回过期的商铺信息
        return r;
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
}
