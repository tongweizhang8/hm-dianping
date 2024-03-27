package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key , Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    public void setWithLogicalExpire(String key , Object value, Long time, TimeUnit unit) {
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    public <R,ID> R queryWithPassThrough(String keyPrefix, ID id , Class<R> type, Function<ID,R> dbFallBack,Long time,TimeUnit unit) {
        //1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(keyPrefix + id);
        //2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //3.存在,直接返回
            R bean = JSONUtil.toBean(shopJson, type);// JSON 格式的数据转换为 Java 对象
            return bean;
        }
        //判断命中是否是空值
        if (shopJson != null) {
            return null;
        }
        //4.不存在,根据id查询数据库
        R r = dbFallBack.apply(id);
        //5.不存在,返回错误
        if (r == null) {
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(keyPrefix+ id,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
            //返回错误信息
            return null;
        }
        //6.存在,写入redis
        this.set(keyPrefix + id,r,time,unit);
        //7.返回
        return r;
    }

    public <R,ID> R queryWithLogicalExpire(String keyPrefix,ID id, Class<R> type,Function<ID,R> dbFallBack,Long time,TimeUnit unit) {
        //1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        //2.判断是否存在
        if (StrUtil.isBlank(shopJson)) {
            //3.存在,直接返回
            return null;
        }
        //命中,需要先把json反序列化为对象,
        RedisData bean = JSONUtil.toBean(shopJson, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) bean.getData(), type);
        LocalDateTime expireTime = bean.getExpireTime();
        //判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //未过期,直接返回店铺信息
            return r;
        }
        //已过期,需要缓存重建
        //缓存重建
        //获取互斥锁
        boolean isLock = tryLock(RedisConstants.LOCK_SHOP_KEY + id);
        //判断是否获取锁成功
        if (isLock) {
            //成功,开启独立线程,实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                //释放锁
                try {
                    //重建缓存
                    //查询数据库
                    R apply = dbFallBack.apply(id);
                    //写入redis
                    this.setWithLogicalExpire(keyPrefix + id, apply,time,unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(RedisConstants.LOCK_SHOP_KEY + id);
                }
            });
        }
        //返回过期的商铺信息
        //7.返回
        return r;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    private boolean tryLock (String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key,"1",10,TimeUnit.MINUTES);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock (String key) {
        stringRedisTemplate.delete(key);
    }
}
