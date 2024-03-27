package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.io.resource.ClassPathResource;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService iSeckillVoucherService;
    @Resource
    private RedisWorker redisWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SEKILL_SCRIPT;

    static {
        SEKILL_SCRIPT = new DefaultRedisScript<>();
        SEKILL_SCRIPT.setLocation((org.springframework.core.io.Resource) new ClassPathResource("unlock.lua"));
        SEKILL_SCRIPT.setResultType(Long.class);
    }

//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024); //初始化为一个 ArrayBlockingQueue 的实例，其容量为 1024 * 1024

    private static final ExecutorService SETKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor(); //创建线程池

    @PostConstruct //在对象创建后，做些准备工作
    private void init() {
        SETKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandle()); //向线程池提交一个任务
    }
    private class VoucherOrderHandle implements Runnable { //实现了Runnable接口，所以该类的实例可以作为一个可运行的任务被提交到线程池中执行

        @Override
        public void run() {
            while (true) {
                try {
                    //获取消息队列中的订单信息 XREADGROUP GROUP G1 C1 COUNNT 1 BLOCK 2000 STREAMS streams.order >
                    List<MapRecord<String, Object, Object>> read = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"), //从一个消费者组（g1）中的一个特定消费者（c1）那里获取数据。
                            StreamReadOptions.empty().block(Duration.ofSeconds(2)), //设置了一个等待时间，在这个时间内系统会等待数据到来。如果没有数据，系统就会在规定的时间后停止等待
                            StreamOffset.create("streams.order", ReadOffset.lastConsumed()) //从指定名称为 streams.order 的流中读取数据，但我只需要从上次处理完的位置开始读取，不需要重复处理已经处理过的数据
                    );
                    //判断消息是否获取成功
                    if (read == null || read.isEmpty()) {
                        //如果获取失败,说明没有消息,继续下一次循环
                        continue;
                    }
                    //如果获取成功,可以下单
                    //解析消息中的订单信息
                    //从Redis消息队列中读取订单信息，并将订单信息转换为VoucherOrder对象进行处理
                    MapRecord<String, Object, Object> record = read.get(0);//从读取的消息队列记录列表read中获取第一条记录
                    Map<Object, Object> value = record.getValue();//将消息记录中的字段-值对提取出来
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);//将前一步提取出的字段-值对Map转换为VoucherOrder对象。
                    //创建订单
                    handleVoucherOrder(voucherOrder);
                    //ack确认
                    stringRedisTemplate.opsForStream().acknowledge("stream.order","g1",record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();//处理异常
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    //获取pending-list中的订单信息 XREADGROUP GROUP G1 C1 COUNNT 1 BLOCK 2000 STREAMS streams.order 0
                    List<MapRecord<String, Object, Object>> read = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"), //Consumer.from创建一个消费者对象
                            StreamReadOptions.empty(),//StreamReadOptions 表示用于配置流读取操作的选项
                            StreamOffset.create("streams.order", ReadOffset.from("0")) ////StreamOffset.create是用于创建一个流偏移量对象的方法。在 Redis Stream 中，偏移量（offset）表示了从流中读取消息的位置
                    );
                    //判断消息是否获取成功
                    if (read == null || read.isEmpty()) {
                        //如果获取失败,说明没有消息,结束循环
                        break;
                    }
                    //如果获取成功,可以下单
                    //解析消息中的订单信息
                    //从Redis消息队列中读取订单信息，并将订单信息转换为VoucherOrder对象进行处理
                    MapRecord<String, Object, Object> record = read.get(0);//从读取的消息队列记录列表read中获取第一条记录
                    Map<Object, Object> value = record.getValue();//将消息记录中的字段-值对提取出来
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);//将前一步提取出的字段-值对Map转换为VoucherOrder对象。
                    //创建订单
                    handleVoucherOrder(voucherOrder);
                    //ack确认
                    stringRedisTemplate.opsForStream().acknowledge("stream.order","g1",record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }

//    private class VoucherOrderHandle implements Runnable { //实现了Runnable接口，所以该类的实例可以作为一个可运行的任务被提交到线程池中执行
//
//        @Override
//        public void run() {
//            while (true) {
//                try {
//                    //获取队列中的订单信息
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    //创建订单
//                    handleVoucherOrder(voucherOrder);
//                } catch (InterruptedException e) {
//                    log.error("处理订单异常", e);
//                } finally {
//                }
//            }
//        }

        private void handleVoucherOrder(VoucherOrder voucherOrder) {
            //1.获取用户
            Long userId = voucherOrder.getUserId();
            //创建锁对象
            //SimpleRedisLock simpleRedisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
            RLock lock = redissonClient.getLock("order:" + userId);
            //获取锁
            boolean tryLock = lock.tryLock(); //确保在执行某些需要独占资源或者保持同步的操作时，只有一个线程可以执行，避免多个线程同时执行导致的并发问题
            //判断是否获取锁成功
            if (!tryLock) {
                log.error("不允许重复下单");
                return;
            }
            try {
                proxy.getResult(voucherOrder); //不需要返回值
            } finally {
                lock.unlock();
            }
        }
    }

    private IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //订单id
        long order = redisWorker.nextId("order");
        //1.执行lua脚本,用于执行秒杀操作
        Long execute = stringRedisTemplate.execute(
                SEKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(),String.valueOf(order)
        );
        //2.判断结果是否为0
        int i = execute.intValue();
        if (i != 0) {
            //3.不为零,没有购买资格
            return Result.fail(i == 1 ? "库存不足" : "不能重复下单");
        }
//        //4.为零,有购买资格,把下单信息保存到阻塞队列
//        VoucherOrder voucherOrder = new VoucherOrder();
//        //保存阻塞队列
//        //订单id
//        voucherOrder.setId(order);
//        //用户id
//        voucherOrder.setId(userId);
//        //代金券id
//        voucherOrder.setVoucherId(voucherId);
//        //放入阻塞队列
//        orderTasks.add(voucherOrder);
        //获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //5.返回订单id
        return Result.ok(order);
    }

    @Transactional
    public void getResult(VoucherOrder voucherOrder) {
        //一人一单
        //Long userId = UserHolder.getUser().getId();
        Long userId = voucherOrder.getUserId();
        //查询订单
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder).count();
        //判断是否存在
        if (count > 0) {
            log.error("库存不足");
            return;
        }
        //6.创建订单
        save(voucherOrder);
//            VoucherOrder voucherOrder = new VoucherOrder();
//            //订单id
//            long order = redisWorker.nextId("order");
//            voucherOrder.setId(order);
//            //用户id
//            voucherOrder.setId(userId);
//            //代金券id
//            voucherOrder.setVoucherId(voucherOrder);
//            save(voucherOrder);
//            //7.返回订单id
//            return Result.ok(order);
    }

    //@Override
//    public Result seckillVoucher(Long voucherId) {
//        //1.查询优惠券
//        SeckillVoucher voucher = iSeckillVoucherService.getById(voucherId);
//        //2.判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀未开始");
//        }
//        //3.判断秒杀是否结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀已结束");
//        }
//        //4.判断库存是否充足
//        if (voucher.getStock() < 1) {
//            return Result.fail("库存不足");
//        }
//        //5.扣减库存
//        boolean success = iSeckillVoucherService.update()
//                .setSql("stock = stock -1") //set stock = stock - 1
//                .eq("voucher_id", voucherId).gt("stock", voucher.getStock()) //where id = ? and stock > 0 //乐观锁,锁指定数量的订单
//                .update();
//        if (!success) {
//            Result.fail("扣减失败");
//        }
//
//        Long userId = UserHolder.getUser().getId();
//        //悲观锁,锁当前用户
////        synchronized (userId.toString()/*转成字符串*/.intern()/*将该字符串放入常量池中，以便后续可以重用相同内容的字符串*/) {
////            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();//获取当前对象的代理对象
////            return proxy.getResult(voucherId); //如果getResult()方法被代理了，那么代理对象中定义的操作将不会执行
////    }
//        //创建锁对象
//        //SimpleRedisLock simpleRedisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("order:" + userId);
//        //获取锁
//        boolean tryLock = lock.tryLock();
//        //判断是否获取锁成功
//        if (!tryLock) {
//            return Result.fail("不允许重复下单");
//        }
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.getResult(voucherId);
//        } finally {
//            lock.unlock();
//        }
//    }

//        @Override
//        public Result seckillVoucher(Long voucherId) {
//            //获取用户
//            Long userId = UserHolder.getUser().getId();
//            //1.执行lua脚本
//            Long execute = stringRedisTemplate.execute(
//                    SEKILL_SCRIPT,
//                    Collections.emptyList(),
//                    voucherId.toString(), userId.toString()
//            );
//            //2.判断结果是否为0
//            int i = execute.intValue();
//            if (i != 0) {
//                //3.不为零,没有购买资格
//                return Result.fail(i == 1 ? "库存不足" : "不能重复下单");
//            }
//            //4.为零,有购买资格,把下单信息保存到阻塞队列
//            VoucherOrder voucherOrder = new VoucherOrder();
//            //保存阻塞队列
//            //订单id
//            long order = redisWorker.nextId("order");
//            voucherOrder.setId(order);
//            //用户id
//            voucherOrder.setId(userId);
//            //代金券id
//            voucherOrder.setVoucherId(voucherId);
//            //放入阻塞队列
//            orderTasks.add(voucherOrder);
//            //获取代理对象
//            proxy = (IVoucherOrderService) AopContext.currentProxy();
//            //5.返回订单id
//            return Result.ok(order);
//        }
}
