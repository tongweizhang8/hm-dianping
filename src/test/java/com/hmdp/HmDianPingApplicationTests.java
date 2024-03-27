package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private CacheClient cacheClient;
    @Resource
    private ShopServiceImpl shopService;
    @Resource
    private RedisWorker redisWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    private ExecutorService es = Executors.newFixedThreadPool(500);

    @Test
    void testIdWorker() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(300); //允许一个或多个线程等待其他线程完成操作
        Runnable task = () -> {
            for (int i = 0;i < 100;i++) {
                long order = redisWorker.nextId("order");
                System.out.println(order);
            }
            countDownLatch.countDown();
        };
        long begin = System.currentTimeMillis();
        for (int i = 0;i < 300;i++) {
            es.submit(task);
        }
        countDownLatch.await(); //等待所有任务执行完成
        long end = System.currentTimeMillis();
        System.out.println(end - begin);
    }

    @Test
    void testSaveShop() {
        Shop shop = shopService.getById(1L);

        cacheClient.setWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY + 1L,shop, 10L, TimeUnit.MINUTES);
    }

    @Test
    void loadShopData() {
        //1.查询店铺信息
        List<Shop> list = shopService.list();
        //2.把店铺分组,按照typeId分组,,typeId一致的放到一个集合
        Map<Long, List<Shop>> collect = list.stream().collect(Collectors.groupingBy(shop -> shop.getTypeId()));
        //3.分批完成写入redis
        for (Map.Entry<Long, List<Shop>> entry : collect.entrySet()) {
            //3.1获取类型id
            Long key = entry.getKey();
            //3.2获取同类型的店铺集合
            List<Shop> value = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(value.size());
            //3.3写入redis GEOAADD key 经度 纬度 member
            for (Shop shop : value) {
                //stringRedisTemplate.opsForGeo().add("shop:geo:" + key, new Point(shop.getX(),shop.getY()),shop.getId().toString());
                locations.add(new RedisGeoCommands.GeoLocation<>(shop.getId().toString(),new Point(shop.getX(),shop.getY())));
            }
            stringRedisTemplate.opsForGeo().add("shop:geo:" + key,locations);
        }
    }

    @Test
    void testHyperLogLog() {
        String[] values = new String[1000];
        int j = 0;
        for (int i = 0;i < 10000;i++) {
            j = i % 1000;
            values[j] = "user_" + i;
            if (j == 999) {
                //发送到redis
                stringRedisTemplate.opsForHyperLogLog().add("hl2",values);
                //HyperLogLog是一种用于近似计数的数据结构，它可以估算集合中不重复元素的数量，而不需要占用大量的内存空间。
            }
        }
        //统计数量
        Long count = stringRedisTemplate.opsForHyperLogLog().size("hl2");
        //size()方法用于获取指定 HyperLogLog 集合中的近似基数，即集合中不同元素的数量的估计值
        System.out.println("count=" + count);
    }

}
