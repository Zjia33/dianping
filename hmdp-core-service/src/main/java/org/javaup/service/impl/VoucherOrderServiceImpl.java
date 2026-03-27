package org.javaup.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.javaup.core.RedisKeyManage;
import org.javaup.core.SpringUtil;
import org.javaup.dto.CancelVoucherOrderDto;
import org.javaup.dto.GetVoucherOrderByVoucherIdDto;
import org.javaup.dto.GetVoucherOrderDto;
import org.javaup.dto.Result;
import org.javaup.dto.VoucherReconcileLogDto;
import org.javaup.entity.SeckillVoucher;
import org.javaup.entity.UserInfo;
import org.javaup.entity.Voucher;
import org.javaup.entity.VoucherOrder;
import org.javaup.entity.VoucherOrderRouter;
import org.javaup.enums.BaseCode;
import org.javaup.enums.BusinessType;
import org.javaup.enums.LogType;
import org.javaup.enums.OrderStatus;
import org.javaup.enums.SeckillVoucherOrderOperate;
import org.javaup.exception.HmdpFrameException;
import org.javaup.kafka.message.SeckillVoucherMessage;
import org.javaup.kafka.producer.SeckillVoucherProducer;
import org.javaup.kafka.redis.RedisVoucherData;
import org.javaup.lua.SeckillVoucherDomain;
import org.javaup.lua.SeckillVoucherOperate;
import org.javaup.mapper.VoucherOrderMapper;
import org.javaup.mapper.VoucherOrderRouterMapper;
import org.javaup.message.MessageExtend;
import org.javaup.model.SeckillVoucherFullModel;
import org.javaup.redis.RedisCacheImpl;
import org.javaup.redis.RedisKeyBuild;
import org.javaup.repeatexecutelimit.annotion.RepeatExecuteLimit;
import org.javaup.service.ISeckillVoucherService;
import org.javaup.service.IUserInfoService;
import org.javaup.service.IVoucherOrderRouterService;
import org.javaup.service.IVoucherOrderService;
import org.javaup.service.IVoucherReconcileLogService;
import org.javaup.service.IVoucherService;
import org.javaup.toolkit.SnowflakeIdGenerator;
import org.javaup.utils.RedisIdWorker;
import org.javaup.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamInfo;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.javaup.constant.Constant.SECKILL_VOUCHER_TOPIC;
import static org.javaup.constant.RepeatExecuteLimitConstants.SECKILL_VOUCHER_ORDER;

/**
 * @description: 优惠券订单服务实现类
 *
 * <p>
 *     核心功能说明：
 *     1. 秒杀优惠券下单（doSeckillVoucherV2）- 当前生产环境使用
 *        - 用户秒杀时，先通过Lua脚本在Redis中扣减库存和记录购买资格
 *        - 扣减成功后，发送MQ消息异步创建数据库订单
 *        - 支持会员等级限制、人群规则校验
 *
 *     2. 订单创建（createVoucherOrderV2）
 *        - 消费MQ消息，创建正式订单
 *        - 包含订单路由记录、Redis缓存、对账日志
 *
 *     3. 订单取消（cancel）
 *        - 用户主动取消订单
 *        - 恢复库存、记录对账、自动发给下一个候选用户
 *
 *     4. 自动发券（autoIssueVoucherToEarliestSubscriber）
 *        - 取消/退款时，自动发放给订阅列表中最早的候选用户
 * </p>
 *
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    /** 优惠券基础服务 */
    @Resource
    private IVoucherService voucherService;

    /** 秒杀优惠券服务 */
    @Resource
    private ISeckillVoucherService seckillVoucherService;

    /** Redis ID生成器（用于订单号） */
    @Resource
    private RedisIdWorker redisIdWorker;

    /** Redis模板 */
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /** Redisson分布式锁客户端 */
    @Resource
    private RedissonClient redissonClient;

    /** 雪花算法ID生成器 */
    @Resource
    private SnowflakeIdGenerator snowflakeIdGenerator;

    /** 秒杀Lua脚本执行器 */
    @Resource
    private SeckillVoucherOperate seckillVoucherOperate;

    /** 秒杀券MQ消息生产者（Kafka） */
    @Resource
    private SeckillVoucherProducer seckillVoucherProducer;

    /** Redis缓存实现 */
    @Resource
    private RedisCacheImpl redisCache;

    /** 订单路由服务 */
    @Resource
    private IVoucherOrderRouterService voucherOrderRouterService;

    /** 用户信息服务 */
    @Resource
    private IUserInfoService userInfoService;

    /** 订单Mapper */
    @Resource
    private VoucherOrderMapper voucherOrderMapper;

    /** 订单路由Mapper */
    @Resource
    private VoucherOrderRouterMapper voucherOrderRouterMapper;

    /** Redis优惠券数据操作（回滚用） */
    @Resource
    private RedisVoucherData redisVoucherData;

    /** 对账日志服务 */
    @Resource
    private IVoucherReconcileLogService voucherReconcileLogService;

    /**
     * 秒杀Lua脚本 - V1版本使用
     * 负责在Redis中执行库存扣减和用户购买记录
     *
     * 返回值说明：
     * - 0: 成功
     * - 1: 库存不足
     * - 2: 已购买
     * - 3: 未开始
     * - 4: 已结束
     * - 5: 系统异常
     */
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    /**
     * 秒杀订单处理线程池 - V1版本使用
     * 单线程后台异步处理订单
     */
    public static final ThreadPoolExecutor SECKILL_ORDER_EXECUTOR =
            new ThreadPoolExecutor(
                    1,                          // 核心线程数
                    1,                          // 最大线程数
                    0L,                         // 空闲线程存活时间
                    TimeUnit.MILLISECONDS,       // 时间单位
                    new LinkedBlockingQueue<>(1024), // 队列容量
                    new NamedThreadFactory("seckill-order-", false), // 线程工厂
                    new ThreadPoolExecutor.CallerRunsPolicy() // 拒绝策略：由调用线程执行
            );

    /**
     * 自定义线程工厂 - 用于创建有意义的线程名称
     */
    private static class NamedThreadFactory implements ThreadFactory {
        private final String namePrefix;  // 线程名前缀
        private final boolean daemon;     // 是否守护线程
        private final AtomicInteger index = new AtomicInteger(1); // 线程序号

        public NamedThreadFactory(String namePrefix, boolean daemon) {
            this.namePrefix = namePrefix;
            this.daemon = daemon;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, namePrefix + index.getAndIncrement());
            t.setDaemon(daemon);
            // 设置未捕获异常处理器
            t.setUncaughtExceptionHandler((thread, ex) ->
                    log.error("未捕获异常，线程={}, err={}", thread.getName(), ex.getMessage(), ex)
            );
            return t;
        }
    }

    /**
     * 初始化方法 - V1版本使用
     * V2版本不再使用Redis Stream方式，改为MQ异步
     */
    @PostConstruct
    private void init(){
        // 这是黑马点评的普通版本，升级版本中不再使用此方式
        //SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    /**
     * 销毁方法 - 优雅关闭线程池
     */
    @PreDestroy
    private void destroy(){
        try {
            SECKILL_ORDER_EXECUTOR.shutdown();
            // 等待最多5秒让任务完成
            if (!SECKILL_ORDER_EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)) {
                // 超时则强制关闭
                SECKILL_ORDER_EXECUTOR.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            SECKILL_ORDER_EXECUTOR.shutdownNow();
        }
    }

    /**
     * V1版本订单处理器 - Redis Stream方式
     *
     * <p>已废弃，V2版本使用MQ异步方式</p>
     *
     * 工作流程：
     * 1. 初始化Stream和Consumer Group
     * 2. 阻塞读取消息（XREADGROUP）
     * 3. 处理订单
     * 4. 确认消息（XACK）
     * 5. 处理Pending列表中的消息
     */
    @Deprecated
    private class VoucherOrderHandler implements Runnable {
        /** Redis Stream队列名称 */
        private final String queueName = "stream.orders";

        /**
         * 主循环：持续从Stream中读取订单消息
         */
        @Override
        public void run() {
            while (true) {
                try {
                    // 0.初始化stream（首次运行创建）
                    initStream();
                    // 1.从Stream中读取订单消息（阻塞2秒）
                    // XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),  // 从g1消费者组的c1消费者读取
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)), // 阻塞2秒，最多读1条
                            StreamOffset.create(queueName, ReadOffset.lastConsumed()) // 从最新消息开始读
                    );
                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 没有消息，继续下一次循环
                        continue;
                    }
                    // 解析数据
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 3.创建订单（加分布式锁）
                    handleVoucherOrder(voucherOrder);
                    // 4.确认消息（告诉Redis这条消息已处理）
                    // XACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    // 处理Pending列表中的消息（之前读取但未确认的）
                    handlePendingList();
                }
            }
        }

        /**
         * 初始化Stream和Consumer Group
         * 首次运行时创建必要的Stream结构
         */
        public void initStream() {
            Boolean exists = stringRedisTemplate.hasKey(queueName);
            if (BooleanUtil.isFalse(exists)) {
                log.info("stream不存在，开始创建stream");
                // 不存在，需要创建Stream和Consumer Group
                // XGROUP CREATE stream.orders g1 $ MKSTREAM
                stringRedisTemplate.opsForStream().createGroup(queueName, ReadOffset.latest(), "g1");
                log.info("stream和group创建完毕");
                return;
            }
            // stream存在，判断group是否存在
            StreamInfo.XInfoGroups groups = stringRedisTemplate.opsForStream().groups(queueName);
            if (groups.isEmpty()) {
                log.info("group不存在，开始创建group");
                // group不存在，创建group
                stringRedisTemplate.opsForStream().createGroup(queueName, ReadOffset.latest(), "g1");
                log.info("group创建完毕");
            }
        }

        /**
         * 处理Pending列表中的消息
         *
         * Pending列表存放：已读取但未确认的消息
         * 可能是处理失败或消费者崩溃的消息
         */
        private void handlePendingList() {
            while (true) {
                try {
                    // 从Pending列表读取（id=0表示读取所有Pending消息）
                    // XREADGROUP GROUP g1 c1 COUNT 1 STREAMS s1 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 没有Pending消息，退出
                        break;
                    }
                    // 解析数据
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 3.创建订单
                    handleVoucherOrder(voucherOrder);
                    // 4.确认消息
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理Pending订单异常", e);
                }
            }
        }
    }

    /**
     * 处理订单 - V1版本使用
     *
     * <p>为每个用户订单加分布式锁，防止重复下单</p>
     *
     * @param voucherOrder 订单信息
     */
    @Deprecated
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getId();  // 注意：这里userId存在id字段
        // 创建分布式锁（按用户维度）
        // SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        // 尝试获取锁
        boolean isLock = lock.tryLock();
        // 判断是否获取锁成功
        if (!isLock) {
            // 获取锁失败，说明有其他请求在处理
            log.error("不允许重复下单");
            return;
        }
        try {
            // 获取代理对象（事务）
            createVoucherOrderV1(voucherOrder);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }

    /** 代理对象引用 - 用于V1版本的事务管理 */
    IVoucherOrderService proxy;

    /**
     * 秒杀优惠券入口方法
     *
     * <p>对外暴露的秒杀下单接口，根据配置选择V1或V2实现</p>
     *
     * @param voucherId 优惠券ID
     * @return 订单ID
     */
    @Override
    public Result<Long> seckillVoucher(Long voucherId) {
        // V1: 同步下单模式（已废弃）
        //return doSeckillVoucherV1(voucherId);
        // V2: MQ异步模式（当前使用）
        return doSeckillVoucherV2(voucherId);
    }

    /**
     * V1版本秒杀下单 - 已废弃
     *
     * <p>工作流程：
     * 1. 执行Lua脚本扣减Redis库存
     * 2. Lua脚本原子性检查库存和购买资格
     * 3. 同步返回订单号（不等待订单创建完成）
     * </p>
     *
     * <p>问题：库存扣减和订单创建是同步的，高并发下性能差</p>
     *
     * @param voucherId 优惠券ID
     * @return 订单ID
     */
    @Deprecated
    public Result<Long> doSeckillVoucherV1(Long voucherId) {
        // 1.获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        // 2.生成订单ID
        long orderId = snowflakeIdGenerator.nextId();

        // 3.执行Lua脚本（原子性扣减库存和记录购买）
        // 脚本会检查：库存是否充足、用户是否已购买、秒杀是否在时间范围内
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),  // keys为空，参数通过args传递
                voucherId.toString(),      // arg1: 优惠券ID
                userId.toString(),         // arg2: 用户ID
                String.valueOf(orderId)    // arg3: 订单ID
        );
        int r = result.intValue();

        // 4.判断Lua脚本执行结果
        if (r != 0) {
            // 不为0，代表没有购买资格
            // r=1: 库存不足
            // r=2: 不能重复下单（已购买）
            // r=3: 未开始
            // r=4: 已结束
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        // 5.获取代理对象（用于事务）
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 6.返回订单id（订单创建是同步的，此时已完成）
        return Result.ok(orderId);
    }

    /**
     * V2版本秒杀下单 - 当前生产使用
     *
     * <p>核心设计思想：异步解耦</p>
     * <p>工作流程：
     * 1. 查询优惠券信息和校验会员等级
     * 2. 加载优惠券库存到Redis（如果未加载）
     * 3. 执行Lua脚本原子性扣减Redis库存和记录购买资格
     * 4. 扣减成功后，发送MQ消息（Kafka/RabbitMQ）
     * 5. MQ消费者异步创建数据库订单
     * </p>
     *
     * <p>优势：
     * - 库存扣减和订单创建完全异步
     * - 订单创建失败可重试
     * - 支持消息延迟检测（丢弃延迟超过10秒的消息）
     * - 完整的对账机制
     * </p>
     *
     * @param voucherId 优惠券ID
     * @return 订单ID
     */
    public Result<Long> doSeckillVoucherV2(Long voucherId) {
        // ========== 第一步：查询优惠券信息 ==========
        // 从数据库查询秒杀券详情（包含开始/结束时间、状态、人群规则等）
        SeckillVoucherFullModel seckillVoucherFullModel = seckillVoucherService.queryByVoucherId(voucherId);

        // ========== 第二步：加载库存到Redis ==========
        // 确保Redis中已有该券的库存数据（Lua脚本依赖Redis库存）
        seckillVoucherService.loadVoucherStock(voucherId);

        // ========== 第三步：获取当前用户 ==========
        Long userId = UserHolder.getUser().getId();

        // ========== 第四步：校验会员等级/人群规则 ==========
        // 检查用户是否满足优惠券的购买条件（如：VIP3以上、银卡会员等）
        // verifyUserLevel(seckillVoucherFullModel, userId);

        // ========== 第五步：生成ID ==========
        long orderId = snowflakeIdGenerator.nextId();      // 订单ID
        long traceId = snowflakeIdGenerator.nextId();     // 追踪ID（用于对账和问题排查）

        // ========== 第六步：构建Lua脚本参数 ==========
        // Redis key列表
        List<String> keys = ListUtil.of(
                // 库存Key：如 seckill:stock:1001
                RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_STOCK_TAG_KEY, voucherId).getRelKey(),
                // 用户购买记录Key：如 seckill:user:1001 -> Set[userId1, userId2]
                RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_USER_TAG_KEY, voucherId).getRelKey(),
                // 追踪日志Key：如 seckill:trace:1001 -> ZSet[traceId -> timestamp]
                RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_TRACE_LOG_TAG_KEY, voucherId).getRelKey()
        );

        // Lua脚本参数（9个参数）
        String[] args = new String[9];
        args[0] = voucherId.toString();                  // 优惠券ID
        args[1] = userId.toString();                     // 用户ID
        args[2] = String.valueOf(LocalDateTimeUtil.toEpochMilli(seckillVoucherFullModel.getBeginTime())); // 开始时间戳
        args[3] = String.valueOf(LocalDateTimeUtil.toEpochMilli(seckillVoucherFullModel.getEndTime()));     // 结束时间戳
        args[4] = String.valueOf(seckillVoucherFullModel.getStatus()); // 状态
        args[5] = String.valueOf(orderId);                // 订单ID
        args[6] = String.valueOf(traceId);               // 追踪ID
        args[7] = String.valueOf(LogType.DEDUCT.getCode()); // 日志类型：扣减
        // 计算TTL：距离结束时间 + 1天缓冲
        long secondsUntilEnd = Duration.between(LocalDateTimeUtil.now(), seckillVoucherFullModel.getEndTime()).getSeconds();
        long ttlSeconds = Math.max(1L, secondsUntilEnd + Duration.ofDays(1).getSeconds());
        args[8] = String.valueOf(ttlSeconds);             // Redis数据过期时间

        // ========== 第七步：执行Lua脚本 ==========
        // 原子性完成：库存扣减、购买记录写入、时间校验
        SeckillVoucherDomain seckillVoucherDomain = seckillVoucherOperate.execute(keys, args);

        // ========== 第八步：检查Lua执行结果 ==========
        if (!seckillVoucherDomain.getCode().equals(BaseCode.SUCCESS.getCode())) {
            // Lua脚本返回非0码，说明扣减失败（库存不足/已购买/不在时间范围）
            throw new HmdpFrameException(Objects.requireNonNull(BaseCode.getRc(seckillVoucherDomain.getCode())));
        }

        // ========== 第九步：发送MQ消息 ==========
        // 扣减Redis成功，发送消息让消费者创建正式订单
        SeckillVoucherMessage seckillVoucherMessage = new SeckillVoucherMessage(
                userId,                                // 用户ID
                voucherId,                             // 优惠券ID
                orderId,                               // 订单ID
                traceId,                               // 追踪ID
                seckillVoucherDomain.getBeforeQty(),   // 扣减前库存
                seckillVoucherDomain.getDeductQty(),   // 扣减数量（通常为1）
                seckillVoucherDomain.getAfterQty(),    // 扣减后库存
                Boolean.FALSE                          // 是否自动发券（手动秒杀为false）
        );

        // 发送消息到MQ，消息格式：Topic + MessageExtend包装
        // Topic命名：prefix + "-" + SECKILL_VOUCHER_TOPIC（如 hmdp-seckill_voucher_topic）
        seckillVoucherProducer.sendPayload(
                SpringUtil.getPrefixDistinctionName() + "-" + SECKILL_VOUCHER_TOPIC,
                seckillVoucherMessage);

        // ========== 第十步：返回订单ID ==========
        // 注意：此时订单还没真正创建（异步创建中）
        return Result.ok(orderId);
    }

    /**
     * 校验用户是否满足优惠券的购买条件
     *
     * <p>支持两种规则：
     * 1. 指定会员级别（allowedLevels）：如 "3,4,5" 表示仅银卡、金卡、白金卡可购买
     * 2. 最低会员级别（minLevel）：如 3 表示VIP3及以上可购买
     * </p>
     *
     * <p>如果不设置任何规则，则所有用户都可购买</p>
     *
     * @param seckillVoucherFullModel 优惠券完整信息
     * @param userId 用户ID
     * @throws HmdpFrameException 如果用户不满足购买条件
     */
    public void verifyUserLevel(SeckillVoucherFullModel seckillVoucherFullModel, Long userId) {
        // 获取优惠券的人群规则
        String allowedLevelsStr = seckillVoucherFullModel.getAllowedLevels(); // 指定级别，如"3,4,5"
        Integer minLevel = seckillVoucherFullModel.getMinLevel();              // 最低级别，如3

        // 判断是否有等级规则
        boolean hasLevelRule = StrUtil.isNotBlank(allowedLevelsStr) || Objects.nonNull(minLevel);

        // 如果没有设置任何规则，直接放行
        if (!hasLevelRule) {
            return;
        }

        // 查询用户信息
        UserInfo userInfo = userInfoService.getByUserId(userId);
        if (Objects.isNull(userInfo)) {
            throw new HmdpFrameException(BaseCode.USER_NOT_EXIST);
        }

        boolean allowed = true;
        Integer level = userInfo.getLevel();  // 用户当前级别

        // ========== 规则1：指定级别检查 ==========
        // 如 allowedLevels="3,4,5" 表示仅3、4、5级会员可购买
        if (StrUtil.isNotBlank(allowedLevelsStr)) {
            try {
                // 解析逗号分隔的级别列表
                Set<Integer> allowedLevels = Arrays.stream(allowedLevelsStr.split(","))
                        .map(String::trim)           // 去空格
                        .filter(StrUtil::isNotBlank) // 过滤空字符串
                        .map(Integer::valueOf)       // 转为Integer
                        .collect(Collectors.toSet());

                if (CollectionUtil.isNotEmpty(allowedLevels)) {
                    // 检查用户级别是否在允许列表中
                    allowed = allowedLevels.contains(level);
                }
            } catch (Exception parseEx) {
                // 解析失败，记录警告但不阻止购买
                log.warn("allowedLevels 解析失败, voucherId={}, raw={}",
                        seckillVoucherFullModel.getVoucherId(),
                        allowedLevelsStr, parseEx);
            }
        }

        // ========== 规则2：最低级别检查 ==========
        // 如 minLevel=3 表示VIP3及以上可购买
        if (allowed && Objects.nonNull(minLevel)) {
            allowed = Objects.nonNull(level) && level >= minLevel;
        }

        // ========== 最终判定 ==========
        if (!allowed) {
            throw new HmdpFrameException("当前会员级别不满足参与条件");
        }
    }

    /**
     * 订单创建 - V1版本
     *
     * <p>直接同步创建订单，不经过MQ</p>
     *
     * @param voucherOrder 订单信息
     */
    @Deprecated
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void createVoucherOrderV1(VoucherOrder voucherOrder) {
        // ========== 第一步：一人一单校验 ==========
        Long userId = voucherOrder.getUserId();

        // 查询该用户是否已购买过该券
        Long count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();

        // ========== 第二步：检查是否重复购买 ==========
        if (count > 0) {
            // 用户已经购买过了
            log.error("用户已经购买过一次！");
            return;
        }

        // ========== 第三步：扣减数据库库存 ==========
        // 注意：这里已经是二次校验了（Lua脚本已校验过一次）
        boolean success = seckillVoucherService.update()
                // SQL: UPDATE seckill_voucher SET stock = stock - 1
                .setSql("stock = stock - 1")
                // 条件: WHERE voucher_id = ? AND stock > 0
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)
                .update();

        if (!success) {
            // 扣减失败（并发情况下库存可能不足）
            log.error("库存不足！");
            return;
        }

        // ========== 第四步：创建订单记录 ==========
        save(voucherOrder);
    }

    /**
     * 订单创建 - V2版本（MQ消费者调用）
     *
     * <p>这是MQ消息的消费者，异步创建正式订单</p>
     *
     * <p>工作流程：
     * 1. 检查订单是否已存在（幂等性）
     * 2. 扣减数据库库存
     * 3. 创建订单主记录
     * 4. 创建订单路由记录（用于按用户/券查询）
     * 5. 缓存订单到Redis（用于快速查询）
     * 6. 记录对账日志
     * </p>
     *
     * <p>注意：
     * - 使用@RepeatExecuteLimit防止消息重复消费
     * - 使用@Transactional保证事务原子性
     * - 失败时Spring事务会自动回滚
     * </p>
     *
     * @param message MQ消息包装（包含SeckillVoucherMessage）
     * @return true创建成功
     */
    @Override
    @RepeatExecuteLimit(name = SECKILL_VOUCHER_ORDER, keys = {"#message.uuid"})
    @Transactional(rollbackFor = Exception.class)
    public boolean createVoucherOrderV2(MessageExtend<SeckillVoucherMessage> message) {
        // ========== 第一步：解析消息 ==========
        SeckillVoucherMessage messageBody = message.getMessageBody();
        Long userId = messageBody.getUserId();

        // ========== 第二步：幂等性检查 ==========
        // 查询是否已存在该用户的正常状态订单
        VoucherOrder normalVoucherOrder = lambdaQuery()
                .eq(VoucherOrder::getVoucherId, messageBody.getVoucherId())
                .eq(VoucherOrder::getUserId, userId)
                .eq(VoucherOrder::getStatus, OrderStatus.NORMAL.getCode())
                .one();

        if (Objects.nonNull(normalVoucherOrder)) {
            // 订单已存在（可能是消息重复发送或之前创建成功）
            log.warn("已存在此订单，voucherId：{},userId：{}", normalVoucherOrder.getVoucherId(), userId);
            // 抛出异常让消息不被ACK，但不影响业务
            throw new HmdpFrameException(BaseCode.VOUCHER_ORDER_EXIST);
        }

        // ========== 第三步：扣减数据库库存 ==========
        // 使用 > 0 条件确保不会扣成负数
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", messageBody.getVoucherId())
                .gt("stock", 0)
                .update();

        if (!success) {
            // 库存不足（理论上不应该发生，因为Lua已经扣过了）
            throw new HmdpFrameException("优惠券库存不足！优惠券id:" + messageBody.getVoucherId());
        }

        // ========== 第四步：创建订单主记录 ==========
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(messageBody.getOrderId());           // 订单ID（复用消息中的ID）
        voucherOrder.setUserId(messageBody.getUserId());        // 用户ID
        voucherOrder.setVoucherId(messageBody.getVoucherId()); // 优惠券ID
        voucherOrder.setCreateTime(LocalDateTimeUtil.now());    // 创建时间
        save(voucherOrder);

        // ========== 第五步：创建订单路由记录 ==========
        // 订单路由表：存储 orderId -> userId, voucherId 的映射关系
        // 用于按userId或voucherId快速查询订单
        VoucherOrderRouter voucherOrderRouter = new VoucherOrderRouter();
        voucherOrderRouter.setId(snowflakeIdGenerator.nextId());   // 路由记录ID
        voucherOrderRouter.setOrderId(voucherOrder.getId());         // 关联订单ID
        voucherOrderRouter.setUserId(userId);                        // 用户ID
        voucherOrderRouter.setVoucherId(voucherOrder.getVoucherId()); // 优惠券ID
        voucherOrderRouter.setCreateTime(LocalDateTimeUtil.now());
        voucherOrderRouter.setUpdateTime(LocalDateTimeUtil.now());
        voucherOrderRouterService.save(voucherOrderRouter);

        // ========== 第六步：缓存订单到Redis ==========
        // 设置60秒过期，用于快速查询
        redisCache.set(RedisKeyBuild.createRedisKey(
                        RedisKeyManage.DB_SECKILL_ORDER_KEY, messageBody.getOrderId()),
                voucherOrder,
                60,
                TimeUnit.SECONDS
        );

        // ========== 第七步：记录对账日志 ==========
        // 用于数据一致性校验和财务对账
        voucherReconcileLogService.saveReconcileLog(
                LogType.DEDUCT.getCode(),              // 日志类型：扣减
                BusinessType.SUCCESS.getCode(),        // 业务类型：成功
                "order created",                        // 详情
                message                                // 原始消息（包含traceId）
        );

        return true;
    }

    /**
     * 根据订单ID查询订单
     *
     * <p>查询顺序：
     * 1. 先查Redis缓存（60秒内）
     * 2. 缓存未命中，查订单路由表
     * 3. 路由表命中，查订单表
     * </p>
     *
     * @param getVoucherOrderDto 查询参数（订单ID）
     * @return 订单ID（如果存在）
     */
    @Override
    public Long getSeckillVoucherOrder(GetVoucherOrderDto getVoucherOrderDto) {
        // ========== 第一步：查Redis缓存 ==========
        VoucherOrder voucherOrder =
                redisCache.get(RedisKeyBuild.createRedisKey(
                                RedisKeyManage.DB_SECKILL_ORDER_KEY,
                                getVoucherOrderDto.getOrderId()),
                        VoucherOrder.class);

        if (Objects.nonNull(voucherOrder)) {
            // 缓存命中
            return voucherOrder.getId();
        }

        // ========== 第二步：查订单路由表 ==========
        // 订单路由表存储了 orderId -> voucherId, userId 的映射
        VoucherOrderRouter voucherOrderRouter =
                voucherOrderRouterService.lambdaQuery()
                        .eq(VoucherOrderRouter::getOrderId, getVoucherOrderDto.getOrderId())
                        .one();

        if (Objects.nonNull(voucherOrderRouter)) {
            // 找到路由记录
            return voucherOrderRouter.getOrderId();
        }

        // 未找到订单
        return null;
    }

    /**
     * 根据用户ID和优惠券ID查询订单
     *
     * <p>用于检查用户是否已购买某优惠券</p>
     *
     * @param getVoucherOrderByVoucherIdDto 查询参数（优惠券ID）
     * @return 订单ID（如果存在）
     */
    @Override
    public Long getSeckillVoucherOrderIdByVoucherId(GetVoucherOrderByVoucherIdDto getVoucherOrderByVoucherIdDto) {
        // 查询该用户是否购买过该券（只查正常状态的订单）
        VoucherOrder voucherOrder = lambdaQuery()
                .eq(VoucherOrder::getUserId, UserHolder.getUser().getId())
                .eq(VoucherOrder::getVoucherId, getVoucherOrderByVoucherIdDto.getVoucherId())
                .eq(VoucherOrder::getStatus, OrderStatus.NORMAL.getCode())
                .one();

        if (Objects.nonNull(voucherOrder)) {
            return voucherOrder.getId();
        }

        return null;
    }

    /**
     * 取消优惠券订单
     *
     * <p>用户主动取消订单的完整流程：
     * 1. 查询订单是否存在
     * 2. 更新订单状态为已取消
     * 3. 记录对账日志
     * 4. 恢复数据库库存
     * 5. 回滚Redis数据（库存、购买记录、追踪日志）
     * 6. 清理用户订阅状态
     * 7. 扣减店铺Top买家的统计
     * 8. 自动发放给订阅列表中最早的候选用户
     * </p>
     *
     * @param cancelVoucherOrderDto 取消订单参数
     * @return true取消成功
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public Boolean cancel(CancelVoucherOrderDto cancelVoucherOrderDto) {
        // ========== 第一步：查询订单 ==========
        // 只能取消正常状态的订单
        VoucherOrder voucherOrder = lambdaQuery()
                .eq(VoucherOrder::getUserId, UserHolder.getUser().getId())
                .eq(VoucherOrder::getVoucherId, cancelVoucherOrderDto.getVoucherId())
                .eq(VoucherOrder::getStatus, OrderStatus.NORMAL.getCode())
                .one();

        if (Objects.isNull(voucherOrder)) {
            // 订单不存在
            throw new HmdpFrameException(BaseCode.SECKILL_VOUCHER_ORDER_NOT_EXIST);
        }

        // ========== 第二步：查询优惠券信息 ==========
        SeckillVoucher seckillVoucher = seckillVoucherService.lambdaQuery()
                .eq(SeckillVoucher::getVoucherId, cancelVoucherOrderDto.getVoucherId())
                .one();

        if (Objects.isNull(seckillVoucher)) {
            throw new HmdpFrameException(BaseCode.SECKILL_VOUCHER_NOT_EXIST);
        }

        // ========== 第三步：更新订单状态 ==========
        // 将订单状态改为已取消
        boolean updateResult = lambdaUpdate()
                .set(VoucherOrder::getStatus, OrderStatus.CANCEL.getCode())
                .set(VoucherOrder::getUpdateTime, LocalDateTimeUtil.now())
                .eq(VoucherOrder::getUserId, UserHolder.getUser().getId())
                .eq(VoucherOrder::getVoucherId, cancelVoucherOrderDto.getVoucherId())
                .update();

        // ========== 第四步：生成追踪ID ==========
        long traceId = snowflakeIdGenerator.nextId();

        // ========== 第五步：构建对账日志 ==========
        VoucherReconcileLogDto voucherReconcileLogDto = new VoucherReconcileLogDto();
        voucherReconcileLogDto.setOrderId(voucherOrder.getId());
        voucherReconcileLogDto.setUserId(voucherOrder.getUserId());
        voucherReconcileLogDto.setVoucherId(voucherOrder.getVoucherId());
        voucherReconcileLogDto.setDetail("cancel voucher order ");
        voucherReconcileLogDto.setBeforeQty(seckillVoucher.getStock());
        voucherReconcileLogDto.setChangeQty(1);           // 恢复1个库存
        voucherReconcileLogDto.setAfterQty(seckillVoucher.getStock() + 1);
        voucherReconcileLogDto.setTraceId(traceId);
        voucherReconcileLogDto.setLogType(LogType.RESTORE.getCode());  // 日志类型：恢复
        voucherReconcileLogDto.setBusinessType(BusinessType.CANCEL.getCode()); // 业务类型：取消
        boolean saveReconcileLogResult = voucherReconcileLogService.saveReconcileLog(voucherReconcileLogDto);

        // ========== 第六步：恢复数据库库存 ==========
        boolean rollbackStockResult = seckillVoucherService.rollbackStock(cancelVoucherOrderDto.getVoucherId());

        // ========== 第七步：汇总结果 ==========
        Boolean result = updateResult && saveReconcileLogResult && rollbackStockResult;

        if (result) {
            // ========== 第八步：回滚Redis数据 ==========
            // 恢复Redis中的库存、删除用户购买记录、删除追踪日志
            redisVoucherData.rollbackRedisVoucherData(
                    SeckillVoucherOrderOperate.YES,    // 需要恢复库存
                    traceId,
                    voucherOrder.getVoucherId(),
                    voucherOrder.getUserId(),
                    voucherOrder.getId(),
                    seckillVoucher.getStock(),         // 回滚前库存
                    1,                                 // 恢复数量
                    seckillVoucher.getStock() + 1     // 回滚后库存
            );

            // ========== 第九步：清理订阅状态 ==========
            // 清除用户在秒杀订阅中的状态
            redisCache.delForHash(
                    RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_SUBSCRIBE_STATUS_TAG_KEY,
                            cancelVoucherOrderDto.getVoucherId()),
                    String.valueOf(voucherOrder.getUserId())
            );

            // ========== 第十步：扣减店铺Top买家统计 ==========
            // 如果该券属于某个店铺，需要从该店铺的日榜中扣减该用户的购买数量
            Voucher voucher = voucherService.getById(voucherOrder.getVoucherId());
            if (Objects.nonNull(voucher)) {
                // 格式：yyyyMMdd
                String day = voucherOrder.getCreateTime().format(DateTimeFormatter.BASIC_ISO_DATE);
                RedisKeyBuild dailyKey = RedisKeyBuild.createRedisKey(
                        RedisKeyManage.SECKILL_SHOP_TOP_BUYERS_DAILY_TAG_KEY,
                        voucher.getShopId(),
                        day
                );
                // 从店铺日榜中扣减该用户1次
                redisCache.incrementScoreForSortedSet(dailyKey, String.valueOf(voucherOrder.getUserId()), -1.0);
            }

            // ========== 第十一步：自动发放给候选用户 ==========
            // 取消的券应该自动发放给订阅列表中等待时间最长的用户
            try {
                autoIssueVoucherToEarliestSubscriber(
                        voucherOrder.getVoucherId(),
                        voucherOrder.getUserId()  // 排除刚退券的用户
                );
            } catch (Exception e) {
                // 自动发券失败不影响主流程，记录日志
                log.warn("自动发券失败，voucherId={}, err=\n{}", voucherOrder.getVoucherId(), e.getMessage());
            }
        }

        return result;
    }

    /**
     * 自动向订阅列表中最早的候选用户发放优惠券
     *
     * <p>触发场景：
     * 1. 用户取消订单后
     * 2. 用户退款后
     * </p>
     *
     * <p>算法说明：
     * 1. 从Redis ZSet中获取订阅列表（按时间戳排序，最早的在最前面）
     * 2. 遍历找到第一个满足条件的用户：
     *    - 不是被排除的用户（excludeUserId）
     *    - 没有购买过该券
     *    - 满足会员等级/人群规则
     * 3. 执行Lua脚本扣减库存
     * 4. 发送MQ消息创建订单
     * </p>
     *
     * @param voucherId 优惠券ID
     * @param excludeUserId 需要排除的用户ID（刚退券的用户）
     * @return true发放成功
     */
    @Override
    public boolean autoIssueVoucherToEarliestSubscriber(final Long voucherId, final Long excludeUserId) {
        // ========== 第一步：查询优惠券信息 ==========
        SeckillVoucherFullModel seckillVoucherFullModel = seckillVoucherService.queryByVoucherId(voucherId);

        // 检查优惠券有效性
        if (Objects.isNull(seckillVoucherFullModel)
                ||
                Objects.isNull(seckillVoucherFullModel.getBeginTime())
                ||
                Objects.isNull(seckillVoucherFullModel.getEndTime())) {
            return false;
        }

        // ========== 第二步：加载库存到Redis ==========
        seckillVoucherService.loadVoucherStock(voucherId);

        // ========== 第三步：查找最早的候选用户 ==========
        String candidateUserIdStr = findEarliestCandidate(voucherId, excludeUserId);

        if (StrUtil.isBlank(candidateUserIdStr)) {
            // 没有找到合适的候选用户
            return false;
        }

        // ========== 第四步：发放给候选用户 ==========
        return issueToCandidate(voucherId, candidateUserIdStr, seckillVoucherFullModel);
    }

    /**
     * 查找最早的候选用户
     *
     * <p>从Redis ZSet（订阅列表）中查找等待时间最长的用户</p>
     *
     * <p>ZSet结构：
     * - Key: seckill:subscribe:zset:{voucherId}
     * - Score: 订阅时间戳
     * - Member: userId
     * </p>
     *
     * <p>过滤条件：
     * 1. 不能是被排除的用户（刚退券的用户）
     * 2. 没有购买过该券（不在购买记录Set中）
     * 3. 满足会员等级规则
     * </p>
     *
     * @param voucherId 优惠券ID
     * @param excludeUserId 需要排除的用户ID
     * @return 候选用户ID字符串，如果没找到返回null
     */
    private String findEarliestCandidate(final Long voucherId, final Long excludeUserId) {
        // 订阅ZSet Key：如 seckill:subscribe:zset:1001
        RedisKeyBuild subscribeZSetKey = RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_SUBSCRIBE_ZSET_TAG_KEY, voucherId);
        // 用户购买记录Set Key：如 seckill:user:1001
        RedisKeyBuild purchasedSetKey = RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_USER_TAG_KEY, voucherId);

        // 分页查找，每页1条
        final long pageCount = 1L;
        long offset = 0L;

        while (true) {
            // ========== 按分数范围查询（按订阅时间排序） ==========
            // 从最早的时间开始查
            Set<ZSetOperations.TypedTuple<String>> page = redisCache.rangeByScoreWithScoreForSortedSet(
                    subscribeZSetKey,
                    Double.NEGATIVE_INFINITY,  // 负无穷（最早）
                    Double.POSITIVE_INFINITY,  // 正无穷（最晚）
                    offset,                     // 偏移量
                    pageCount,                   // 数量
                    String.class
            );

            // 没有更多订阅用户了
            if (CollectionUtil.isEmpty(page)) {
                return null;
            }

            // 获取当前页的第一条
            ZSetOperations.TypedTuple<String> tuple = page.iterator().next();

            // 防御性检查
            if (Objects.isNull(tuple) || Objects.isNull(tuple.getValue())) {
                offset++;
                continue;
            }

            String uidStr = tuple.getValue();

            // 过滤空值
            if (StrUtil.isBlank(uidStr)) {
                offset++;
                continue;
            }

            // ========== 过滤1：排除被排除的用户 ==========
            if (Objects.nonNull(excludeUserId) && Objects.equals(uidStr, String.valueOf(excludeUserId))) {
                offset++;
                continue;
            }

            // ========== 过滤2：检查是否已购买 ==========
            // 从购买记录Set中检查该用户是否已购买
            Boolean purchased = redisCache.isMemberForSet(purchasedSetKey, uidStr);
            if (BooleanUtil.isTrue(purchased)) {
                // 该用户已购买，跳过
                offset++;
                continue;
            }

            // 找到合适的候选用户
            return uidStr;
        }
    }

    /**
     * 向候选用户发放优惠券
     *
     * <p>工作流程：
     * 1. 校验用户是否满足会员等级规则
     * 2. 执行Lua脚本扣减Redis库存
     * 3. 发送MQ消息异步创建订单
     * </p>
     *
     * @param voucherId 优惠券ID
     * @param candidateUserIdStr 候选用户ID字符串
     * @param seckillVoucherFullModel 优惠券信息
     * @return true发放成功
     */
    private boolean issueToCandidate(final Long voucherId,
                                      final String candidateUserIdStr,
                                      final SeckillVoucherFullModel seckillVoucherFullModel) {
        Long candidateUserId = Long.valueOf(candidateUserIdStr);

        // ========== 第一步：校验会员等级 ==========
        try {
            verifyUserLevel(seckillVoucherFullModel, candidateUserId);
        } catch (Exception e) {
            // 候选用户不满足人群规则，跳过
            log.info("候选用户不满足人群规则，自动发券跳过。voucherId={}, userId={}", voucherId, candidateUserId);
            return false;
        }

        // ========== 第二步：构建Lua脚本参数 ==========
        List<String> keys = buildSeckillKeys(voucherId);
        long orderId = snowflakeIdGenerator.nextId();
        long traceId = snowflakeIdGenerator.nextId();
        String[] args = buildSeckillArgs(voucherId, candidateUserIdStr, seckillVoucherFullModel, orderId, traceId);

        // ========== 第三步：执行Lua脚本 ==========
        SeckillVoucherDomain domain = seckillVoucherOperate.execute(keys, args);

        if (!Objects.equals(domain.getCode(), BaseCode.SUCCESS.getCode())) {
            // Lua扣减失败（库存可能不足）
            log.info("自动发券Lua扣减失败，code={}, voucherId={}, userId={}", domain.getCode(), voucherId, candidateUserId);
            return false;
        }

        // ========== 第四步：发送MQ消息 ==========
        // 注意：这是自动发券，autoIssue=true
        SeckillVoucherMessage message = new SeckillVoucherMessage(
                candidateUserId,
                voucherId,
                orderId,
                traceId,
                domain.getBeforeQty(),
                domain.getDeductQty(),
                domain.getAfterQty(),
                Boolean.TRUE  // 自动发券标志为true
        );

        seckillVoucherProducer.sendPayload(
                SpringUtil.getPrefixDistinctionName() + "-" + SECKILL_VOUCHER_TOPIC,
                message
        );

        return true;
    }

    /**
     * 构建秒杀Lua脚本的Key列表
     *
     * @param voucherId 优惠券ID
     * @return Key列表
     */
    private List<String> buildSeckillKeys(final Long voucherId) {
        String stockKey = RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_STOCK_TAG_KEY, voucherId).getRelKey();
        String userKey = RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_USER_TAG_KEY, voucherId).getRelKey();
        String traceKey = RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_TRACE_LOG_TAG_KEY, voucherId).getRelKey();
        return ListUtil.of(stockKey, userKey, traceKey);
    }

    /**
     * 构建秒杀Lua脚本的参数
     *
     * @param voucherId 优惠券ID
     * @param userIdStr 用户ID字符串
     * @param seckillVoucherFullModel 优惠券信息
     * @param orderId 订单ID
     * @param traceId 追踪ID
     * @return 参数数组
     */
    private String[] buildSeckillArgs(final Long voucherId,
                                       final String userIdStr,
                                       final SeckillVoucherFullModel seckillVoucherFullModel,
                                       final long orderId,
                                       final long traceId) {
        String[] args = new String[9];
        args[0] = voucherId.toString();
        args[1] = userIdStr;
        args[2] = String.valueOf(LocalDateTimeUtil.toEpochMilli(seckillVoucherFullModel.getBeginTime()));
        args[3] = String.valueOf(LocalDateTimeUtil.toEpochMilli(seckillVoucherFullModel.getEndTime()));
        args[4] = String.valueOf(seckillVoucherFullModel.getStatus());
        args[5] = String.valueOf(orderId);
        args[6] = String.valueOf(traceId);
        args[7] = String.valueOf(LogType.DEDUCT.getCode());
        args[8] = String.valueOf(computeTtlSeconds(seckillVoucherFullModel));
        return args;
    }

    /**
     * 计算Redis数据的TTL
     *
     * <p>TTL = 距离结束时间 + 1天缓冲期</p>
     * <p>确保在秒杀结束后，数据还能保留一段时间用于排查问题</p>
     *
     * @param seckillVoucherFullModel 优惠券信息
     * @return TTL秒数
     */
    private long computeTtlSeconds(final SeckillVoucherFullModel seckillVoucherFullModel) {
        // 距离结束时间
        long secondsUntilEnd = Duration.between(LocalDateTimeUtil.now(), seckillVoucherFullModel.getEndTime()).getSeconds();
        // 加上1天缓冲期，最小为1秒
        return Math.max(1L, secondsUntilEnd + Duration.ofDays(1).getSeconds());
    }

    // ==================== 以下是废弃的旧版本代码 ====================

    /*
     * V0版本：BlockingQueue方式 - 已废弃
     *
     * 问题：单机内存队列，无法跨JVM，分布式部署会丢单
     */
    /* @Deprecated
    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    @Deprecated
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    // 1.从阻塞队列获取订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 2.创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    } */

    /**
     * V0版本秒杀方法 - BlockingQueue方式
     */
    /* @Deprecated
    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        );
        int r = result.intValue();
        // 2.判断结果是否为0
        if (r != 0) {
            // 不为0，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        // 为0，有购买资格，把下单信息保存到阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        // 订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 用户id
        voucherOrder.setUserId(userId);
        // 代金券id
        voucherOrder.setVoucherId(voucherId);
        // 放入阻塞队列
        orderTasks.add(voucherOrder);
        // 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 返回订单id
        return Result.ok(orderId);
    } */

    /**
     * V0版本下单方法 - 直接数据库扣减
     *
     * <p>问题：
     * 1. 数据库单点，无法水平扩展
     * 2. 库存字段竞争激烈，性能差
     * 3. 无异步化，高并发下用户体验差
     * </p>
     */
    /* @Deprecated
    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 1.一人一单
        Long userId = UserHolder.getUser().getId();

        synchronized (userId.toString().intern()) {
            // 1.1查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 1.2判断是否存在
            if (count > 0) {
                return Result.fail("用户已经购买过一次！");
            }

            // 2.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherId).gt("stock", 0)
                    .update();
            if (!success) {
                return Result.fail("库存不足！");
            }

            // 3.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            voucherOrder.setUserId(userId);
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);

            // 返回订单id
            return Result.ok(orderId);
        }
    } */
}
