package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result sendCode(String phone, HttpSession session) {
        //1.校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            //2.如果不符合`,返回错误信息
            return Result.fail("手机号格式不正确");
        }
        //3.符合,生成验证码
        String random = RandomUtil.randomNumbers(6);
//        //4.保存验证码到session
//        session.setAttribute("code", random);
        //4.保存验证码到redis
        stringRedisTemplate.opsForValue().set(RedisConstants.LOGIN_CODE_KEY + phone, random,RedisConstants.LOGIN_CODE_TTL, TimeUnit.MINUTES);
        //5.发送验证码
        log.info("向手机号{}发送验证码:{}", phone, random);
        //6.返回ok
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //1.校验手机号
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号不一致");
        }
//        //2.校验验证码
//        Object catchCode = session.getAttribute("code");
        //2.从redis获取验证码并校验
        String catchCode = stringRedisTemplate.opsForValue().get(RedisConstants.LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if (catchCode == null || !catchCode.equals(code)) {
            //3.不一致,报错
            return Result.fail("验证码不正确");
        }
        //4.一致,根据手机号查询用户
        User user = query().eq("phone", phone).one();
        //5.判断用户是否存在
        if (user == null) {
            //6.不存在,创建新用户并保存
            user = createUserWithPhone(phone);
        }
//        //7.保存用户信息到session中
//        session.setAttribute("user", BeanUtil.copyProperties("user", UserDTO.class));
        //7.保存用户信息到redis
        //7.1随机生成token,作为登陆令牌 uuid
        String token = UUID.randomUUID().toString();
        //7.2将user对象转为hashMap存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> beanToMap = BeanUtil.beanToMap(userDTO,new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName,fieldValue) -> fieldValue.toString()));
        //7.3存储
        stringRedisTemplate.opsForHash().putAll(RedisConstants.LOGIN_USER_KEY + token, beanToMap);
        stringRedisTemplate.expire(RedisConstants.LOGIN_USER_KEY + beanToMap + token,RedisConstants.CACHE_SHOP_TTL,TimeUnit.MINUTES);
        //8.返回token
        return Result.ok(token);
    }

    @Override
    public Result sign() {
        //1.获取当前登陆的用户
        Long id = UserHolder.getUser().getId();
        //2.获取日期
        LocalDateTime now = LocalDateTime.now(); //获取当前的日期时间
        //3.拼接key
        String keys = now.format(DateTimeFormatter.ofPattern(":yyyymm")); //将其格式化为以年份后两位和月份组成的字符串
        // DateTimeFormatter.ofPattern()静态方法创建了一个日期时间格式化器，以指定日期时间的格式
        String key = RedisConstants.USER_SIGN_KEY + id + keys;
        //4.获取今天是本月第几天
        int dayOfMonth = now.getDayOfMonth();
        //5.写入redis SETBIT key offset 1/0
        stringRedisTemplate.opsForValue().setBit(key,dayOfMonth - 1,true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        //获取当前登陆用户
        Long id = UserHolder.getUser().getId();
        //获取日期
        LocalDateTime now = LocalDateTime.now();
        //拼接key
        String yyyymm = now.format(DateTimeFormatter.ofPattern("yyyymm"));
        String key = RedisConstants.USER_SIGN_KEY + id + yyyymm;
        //获取今天是本月第几天
        int dayOfMonth = now.getDayOfMonth();
        //获取本月截止今天为止的所有签到记录,返回的是一个十进制的数字 BITFIELD sign 5:202203 GET u14 0
        List<Long> list = stringRedisTemplate.opsForValue().bitField(
                key,
                BitFieldSubCommands.create().get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0)
                //unsigned()即只能表示非负数 valueAt()表示获取指定偏移量处的位的值 BitFieldSubCommands.create()返回的对象是用于创建位域操作命令的构建器对象
        );
        if (list == null || list.isEmpty()) {
            //没有任何签到结果
            return Result.ok(0);
        }
        Long num = list.get(0);
        if (num == 0) {
            return Result.ok(0);
        }
        //循环遍历
        int count = 0;
        while (true) {
            //让这个数字与1做与运算,得到数字的最后一个bit位,判断这个bit位是否为0
            if ((num & 1) == 0) {
                //如果为0,说明未签到,结束
                break;
            }else {
                //如果不为零,说明已签到,计数器+1
                count++;
            }
            //把数字右移一位,抛弃最后一个bit位,继续下一个bit位
            num >>>= 1; //>>>是Java中的位无符号右移操作符，用于将二进制表示的数向右移动指定的位数，右移时使用零填充。
        }
        return Result.ok(count);
    }

    private User createUserWithPhone(String phone) {
        //1.创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX + RandomUtil.randomString(6));
        //2.保存用户
        save(user);
        return user;
    }
}
