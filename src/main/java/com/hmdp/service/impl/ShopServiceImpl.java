package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import jodd.util.StringUtil;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop = cacheClient.queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //互斥锁解决缓存击穿
        //Shop shop= queryWithMutex(id);
        //逻辑过滤器解决缓存击穿
        //从缓存中查询商店信息
        Shop shop = cacheClient.queryWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //cacheClient.queryWithLogicalExpire(): 这是一个调用缓存客户端（cacheClient）的方法;RedisConstants.CACHE_SHOP_KEY: 这是用于缓存商店信息的键（key）
        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        //7.返回
        return Result.ok(shop);
    }

//    public Shop queryWithLogicalExpire(Long id) {
//        //1.从redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
//        //2.判断是否存在
//        if (StrUtil.isBlank(shopJson)) {
//            //3.存在,直接返回
//            return null;
//        }
//        //命中,需要先把json反序列化为对象,
//        RedisData bean = JSONUtil.toBean(shopJson, RedisData.class);
//        Shop shop = JSONUtil.toBean((JSONObject) bean.getData(), Shop.class);
//        LocalDateTime expireTime = bean.getExpireTime();
//        //判断是否过期
//        if (expireTime.isAfter(LocalDateTime.now())) {
//            //未过期,直接返回店铺信息
//            return shop;
//        }
//        //已过期,需要缓存重建
//        //缓存重建
//        //获取互斥锁
//        boolean isLock = tryLock(RedisConstants.LOCK_SHOP_KEY, id);
//        //判断是否获取锁成功
//        if (isLock) {
//            //成功,开启独立线程,实现缓存重建
//            CACHE_REBUILD_EXECUTOR.submit(() -> {
//                this.saveShop2Redis(id,30L);
//                //释放锁
//                try {
//                    //重建缓存
//                    this.saveShop2Redis(id,30L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                } finally {
//                    unlock(RedisConstants.LOCK_SHOP_KEY, id);
//                }
//            });
//        }
//        //返回过期的商铺信息
//        //7.返回
//        return shop;
//    }
//
//    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public Shop queryWithMutex(Long id) {
        //1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        //2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //3.存在,直接返回
            Shop bean = JSONUtil.toBean(shopJson, Shop.class);// JSON 格式的数据转换为 Java 对象
            return bean;
        }
        Shop shop = null;
        try {
            //实现缓存重建
            //1.获取互斥锁
            boolean isLock = tryLock(RedisConstants.LOCK_SHOP_KEY + id, id);
            //2.判断是否获取成功
            if (!isLock) {
                //3.失败,则休眠并重试
                Thread.sleep(50);
                queryWithMutex(id);
            }
            //4.成功,根据id查询数据库
            shop = getById(id);
            //判断命中是否是空值
            if (shopJson != null) {
                return null;
            }
            //5.不存在,返回错误
            if (shop == null) {
                //将空值写入redis
                stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
                //返回错误信息
                return null;
            }
            //6.存在,写入redis
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(shop),RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES); // Java 对象转换为 JSON 格式
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //释放互斥锁
            unlock(RedisConstants.LOCK_SHOP_KEY + id, id);
            //7.返回
            return shop;
        }
    }

//    public Shop queryWithPassThrough(Long id) {
//        //1.从redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
//        //2.判断是否存在
//        if (StrUtil.isNotBlank(shopJson)) {
//            //3.存在,直接返回
//            Shop bean = JSONUtil.toBean(shopJson, Shop.class);// JSON 格式的数据转换为 Java 对象
//            return bean;
//        }
//        //4.不存在,根据id查询数据库
//        Shop shop = getById(id);
//        //判断命中是否是空值
//        if (shopJson != null) {
//            return null;
//        }
//        //5.不存在,返回错误
//        if (shop == null) {
//            //将空值写入redis
//            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
//            //返回错误信息
//            return null;
//        }
//        //6.存在,写入redis
//        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(shop),RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES); // Java 对象转换为 JSON 格式
//        //7.返回
//        return shop;
//    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("id不存在");
        }
        //1.更新数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1.判断是否需要根据坐标查询
        if (x == null || y == null) {
            //不需要坐标查询,按数据库查询
            Page<Shop> page = query().eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE)); //current：表示当前所请求的页数。pageSize：表示每页要显示的记录数
            return Result.ok(page.getRecords());
        }
        //2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        //3.查询redis,按照距离排序,分页.结果:shopId,distance
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo() //GEOSEARCH key BYLONLAT x y BYARDIUS 10 WITHDISTANCE
                .search(
                        RedisConstants.SHOP_GEO_KEY, //key,按照类型分
                        GeoReference.fromCoordinate(x, y), //圆心
                        new Distance(1000), //半径
                        RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs().includeCoordinates().limit(end)
                        //RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs(): 这是创建一个新的 GeoRadiusCommandArgs 对象，用于配置搜索参数
                        //includeCoordinates(): 这个方法用于指定在搜索结果中包含地理位置的坐标信息
                        //limit(end): 这个方法用于限制搜索结果的数量，参数 end 指定了结果的上限
                );
        //4.解析出id
        if (results == null) {
            return Result.ok();
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> content = results.getContent(); //获得搜索结果的实际内容
        //截取from~end部分
        List<Long> ids = new ArrayList<>(content.size());
        Map<String,Distance> distanceMap = new HashMap<>(content.size());
        content.stream().skip(from).forEach(result -> { //forEach()用于对流中的每个元素执行特定的操作
            //获取店铺id
            String name = result.getContent().getName();
            ids.add(Long.valueOf(name));
            //获取距离
            Distance distance = result.getDistance();
            distanceMap.put(name,distance); //将给定的键值对添加到Map中
    });
        //5.根据id查询shop
        String idStr = StrUtil.join(",",ids);
        List<Shop> list = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        //in:用于指定在id列中包含在ids列表中的值。这个方法会生成一个SQL中的IN子句
        for (Shop shop : list) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue()); //从distanceMap中获取商店的距离信息，并将其设置为商店对象的距离属性
        }
        //6.返回
        return Result.ok(list);
    }

    private void saveShop2Redis(Long id,Long expireSeconds) {
        //1.查询店铺数据
        Shop byId = getById(id);
        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(byId);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3.写入redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(redisData));
    }

    private boolean tryLock (String key, Long id) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key,"1",10,TimeUnit.MINUTES);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock (String key, Long id) {
        stringRedisTemplate.delete(key);
    }
}
