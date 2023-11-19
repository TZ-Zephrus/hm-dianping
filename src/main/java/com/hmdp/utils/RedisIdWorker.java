package com.hmdp.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {

    private static final long BEGIN_TIMESTAMP = 1640995200L;
    //序列号位数
    private static final int COUNT_BITS = 32;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    public long nextId(String keyPrefix) {
        //1生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timeStamp = nowSecond - BEGIN_TIMESTAMP;
        //2生成序列号
        //要注意 同一个业务不能一直用一个key, 因为有可能会超上限。因此拼一个日期
        //获取当天日期
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        //会自动创建key 因此不会空指针
        long count = stringRedisTemplate.opsForValue().increment("icar:" + keyPrefix + ":" + date);
        //3拼接并返回
        //把时间戳往左移动32位2进制数
        long l = timeStamp << COUNT_BITS | count;
        return l;
    }

    public static void main(String[] args) {
        LocalDateTime localDateTime = LocalDateTime.of(2022, 1, 1, 0, 0, 0);
        long time = localDateTime.toEpochSecond(ZoneOffset.UTC);
        System.out.println(time);
    }
}
