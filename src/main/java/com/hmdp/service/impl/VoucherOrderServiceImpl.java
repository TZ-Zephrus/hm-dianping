package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Autowired
    private ISeckillVoucherService seckillVoucherService;

    @Autowired
    private RedisIdWorker redisIdWorker;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private RedissonClient redissonClient;

    //加载lua脚本
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckillWithStream.lua"));
//        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    //创建线程池
    private final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();


    //用Stream消息队列
    //创建线程任务 用内部类的方式
    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";

        @Override
        public void run() {
            while (true) {
                try {
                    //1. 获取消息队列中订单信息 XREADGROUP GROUP g1 c1 count 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //2 判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        //2.1 失败 无消息 进入下一次循环
                        continue;
                    }
                    //3 解析消息中的订单信息
                    MapRecord<String, Object, Object> entries = list.get(0);
                    Map<Object, Object> values = entries.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //4. 获取成功创建订单
                    handleVoucherOrder(voucherOrder);
                    //5. ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", entries.getId());

                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handelPendingList();
                }
            }
        }

        private void handelPendingList() {
            while (true) {
                try {
                    //1. 获取Pending list中订单信息 XREADGROUP GROUP g1 c1 count 1 BLOCK 2000 STREAMS streams.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //2 判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        //2.1 失败 无消息 说明pending list无异常
                        break;
                    }
                    //3 解析消息中的订单信息
                    MapRecord<String, Object, Object> entries = list.get(0);
                    Map<Object, Object> values = entries.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //3. 获取成功创建订单
                    handleVoucherOrder(voucherOrder);
                    //5. ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", entries.getId());
                } catch (Exception e) {
                    log.error("处理pending list异常", e);
                }
            }
        }
    }



    //用Stream队列
    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //获取订单id
        long orderId = redisIdWorker.nextId("order");
        //1执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId));
        //2判断结果是否为0
        int r = result.intValue();
        if (r != 0) {
            //2.1 不为0 没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        //2.2 为0 有购买资格
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        //3返回订单id
        return Result.ok(orderId);
    }


    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //不是主线程，不能从UserHolder中取userId
//        Long userId = UserHolder.getUser().getId();
        //1. 只能从voucherOrder中取
        Long userId = voucherOrder.getUserId();


        //2. 创建锁对象
        //Reddison锁
        RLock simpleRedisLock = redissonClient.getLock("order:" + userId);
        //3. 获取锁  这里不加锁也可以
//        boolean isLock = simpleRedisLock.tryLock(1200);
        boolean isLock = simpleRedisLock.tryLock();  //默认 失败不等待
        //4. 判断是否获取锁成功
        if (!isLock) {
            //获取锁失败，返回错误或重试
            log.error("不允许重复下单");
        }
        try {
            //获取代理对象(spring中事务的机制)  这里也是拿不到
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            //释放锁
            simpleRedisLock.unlock();
        }
    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();

        //这里很多逻辑其实不需要

        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        // 5.1.查询订单
        LambdaQueryWrapper<VoucherOrder> lambdaQueryWrapper = new LambdaQueryWrapper<>();
        lambdaQueryWrapper.eq(VoucherOrder::getUserId, userId)
                .eq(VoucherOrder::getVoucherId, voucherId);
        long count = this.count(lambdaQueryWrapper);

        // 5.2.判断是否存在
        if (count > 0) {
            // 用户已经购买过了
            log.error("用户已经购买过一次！");
            return;
        }

        //6 扣减库存
        //使用乐观锁，防止超卖！
        seckillVoucher.setStock(seckillVoucher.getStock() - 1);
        LambdaUpdateWrapper<SeckillVoucher> lambdaUpdateWrapper = new LambdaUpdateWrapper<>();
        lambdaUpdateWrapper
                .gt(SeckillVoucher::getStock, 0)
                .eq(SeckillVoucher::getVoucherId, voucherId);
        boolean success = seckillVoucherService.update(seckillVoucher, lambdaUpdateWrapper);
//        boolean success = seckillVoucherService
//                .update()
//                .setSql("stock = stock-1")
//                .eq("voucher_id", voucherId)
//                .eq("stock", voucher.getStock())
//                .update();
        if (!success) {
            // 扣减失败
            log.error("库存不足！");
            return;
        }
        save(voucherOrder);
    }

    //注解表示在spring初始化后，就开始执行。
    //目的是为了在项目启动后立即开始执行阻塞队列处理功能
    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    //代理对象
    private IVoucherOrderService proxy;






    //这里用lua脚本，在redis里查订单判断能否下单
    //使用阻塞队列的seckillVoucher
/*  以下为阻塞队列

    //旧的Class VoucherOrderHandler
    //创建阻塞队列
private final BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024*1024);
//创建线程任务 用内部类的方式
private class VoucherOrderHandler implements Runnable{
    @Override
    public void run() {
        while (true) {

            try {
                //1. 获取队列中订单信息
                //阻塞方法
                VoucherOrder voucherOrder = orderTasks.take();
                //2. 创建订单
                handleVoucherOrder(voucherOrder);
            } catch (InterruptedException e) {
                log.error("处理订单异常", e);
            }
        }
    }
}

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //不是主线程，不能从UserHolder中取userId
//        Long userId = UserHolder.getUser().getId();
        //1. 只能从voucherOrder中取
        Long userId = voucherOrder.getUserId();


        //2. 创建锁对象
        //Reddison锁
        RLock simpleRedisLock = redissonClient.getLock("order:" + userId);
        //3. 获取锁  这里不加锁也可以
//        boolean isLock = simpleRedisLock.tryLock(1200);
        boolean isLock = simpleRedisLock.tryLock();  //默认 失败不等待
        //4. 判断是否获取锁成功
        if (!isLock) {
            //获取锁失败，返回错误或重试
            log.error("不允许重复下单");
        }
        try {
            //获取代理对象(spring中事务的机制)  这里也是拿不到
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            //释放锁
            simpleRedisLock.unlock();
        }
    }

    //注解表示在spring初始化后，就开始执行。
    //目的是为了在项目启动后立即开始执行阻塞队列处理功能
    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }


    //代理对象
    private IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        //1执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString());
        //2判断结果是否为0
        int r = result.intValue();
        if (r != 0) {
            //2.1 不为0 没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        //2.2 为0 有购买资格 把下单信息保存到阻塞队列
        //保存阻塞队列
        // 创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        // 2.3 订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 2.4 用户id
        voucherOrder.setUserId(userId);
        // 2.5 代金券id
        voucherOrder.setVoucherId(voucherId);
        // 2.6 创建阻塞队列
        // 子线程拿不到代理对象 这里把代理对象扔进去
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        orderTasks.add(voucherOrder);

        //3返回订单id
        return Result.ok(orderId);
    }




        @Transactional
        public void createVoucherOrder(VoucherOrder voucherOrder) {
            Long userId = voucherOrder.getUserId();
            Long voucherId = voucherOrder.getVoucherId();

            //这里很多逻辑其实不需要

            SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
            // 5.1.查询订单
            LambdaQueryWrapper<VoucherOrder> lambdaQueryWrapper = new LambdaQueryWrapper<>();
            lambdaQueryWrapper.eq(VoucherOrder::getUserId, userId)
                    .eq(VoucherOrder::getVoucherId, voucherId);
            long count = this.count(lambdaQueryWrapper);

            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                log.error("用户已经购买过一次！");
                return;
            }

            //6 扣减库存
            //使用乐观锁，防止超卖！
            seckillVoucher.setStock(seckillVoucher.getStock() - 1);
            LambdaUpdateWrapper<SeckillVoucher> lambdaUpdateWrapper = new LambdaUpdateWrapper<>();
            lambdaUpdateWrapper
                    .gt(SeckillVoucher::getStock, 0)
                    .eq(SeckillVoucher::getVoucherId, voucherId);
            boolean success = seckillVoucherService.update(seckillVoucher, lambdaUpdateWrapper);
//        boolean success = seckillVoucherService
//                .update()
//                .setSql("stock = stock-1")
//                .eq("voucher_id", voucherId)
//                .eq("stock", voucher.getStock())
//                .update();
            if (!success) {
                // 扣减失败
                log.error("库存不足！");
                return;
            }
            save(voucherOrder);
        }


    //以上为阻塞队列
*/



    /*@Transactional
    public  Result createVoucherOrder(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        // 5.1.查询订单
        LambdaQueryWrapper<VoucherOrder> lambdaQueryWrapper = new LambdaQueryWrapper<>();
        lambdaQueryWrapper.eq(VoucherOrder::getUserId, userId)
                .eq(VoucherOrder::getVoucherId, voucherId);
        long count = this.count(lambdaQueryWrapper);

        // 5.2.判断是否存在
        if (count > 0) {
            // 用户已经购买过了
            return Result.fail("用户已经购买过一次！");
        }

        //6 扣减库存
        //使用乐观锁，防止超卖！
        seckillVoucher.setStock(seckillVoucher.getStock()-1);
        LambdaUpdateWrapper<SeckillVoucher> lambdaUpdateWrapper = new LambdaUpdateWrapper<>();
        lambdaUpdateWrapper
                .gt(SeckillVoucher::getStock, 0)
                .eq(SeckillVoucher::getVoucherId, voucherId);
        boolean success = seckillVoucherService.update(seckillVoucher, lambdaUpdateWrapper);
//        boolean success = seckillVoucherService
//                .update()
//                .setSql("stock = stock-1")
//                .eq("voucher_id", voucherId)
//                .eq("stock", voucher.getStock())
//                .update();
        if (!success) {
            // 扣减失败
            return Result.fail("库存不足！");
        }

        // 7.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        // 7.1.订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 7.2.用户id
        voucherOrder.setUserId(userId);
        // 7.3.代金券id
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);

        // 7.返回订单id
        return Result.ok(orderId);
    }*/



    //比较原始的 用mysql查
    /*@Override
    @Transactional   //设计多张表，加上事务
    public Result seckillVoucher(Long voucherId) {
        //1 查询优惠券
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        //2 判断秒杀是否开始
        if (seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀尚未开始呢~");
        }
        //3 判断秒杀是否结束
        if (seckillVoucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已经结束惹~");
        }
        //4 判断库存是否充足
        if (seckillVoucher.getStock() < 1) {
            return Result.fail("库存不足了呜呜");
        }

//        //这里有讲究 看笔记
//        //用悲观锁解决一人一单问题  只锁同一个用户
//        Long userId = UserHolder.getUser().getId();
//        synchronized(userId.toString().intern()){
//            //获取代理对象(spring中事务的机制)
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }


        Long userId = UserHolder.getUser().getId();
        //创建锁对象
            //自己定义的锁
//        SimpleRedisLock simpleRedisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
            //Reddison锁
        RLock simpleRedisLock = redissonClient.getLock("order:" + userId);
        //获取锁
//        boolean isLock = simpleRedisLock.tryLock(1200);
        boolean isLock = simpleRedisLock.tryLock();  //默认 失败不等待
        //判断是否获取锁成功
        if (!isLock) {
            //获取锁失败，返回错误或重试
            return Result.fail("不允许重复下单");
        }
        try {
            //获取代理对象(spring中事务的机制)
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            //释放锁
            simpleRedisLock.unlock();
        }
    }*/

}
