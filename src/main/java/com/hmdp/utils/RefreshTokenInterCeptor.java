package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RefreshTokenInterCeptor implements HandlerInterceptor {

    private StringRedisTemplate stringRedisTemplate;

    public RefreshTokenInterCeptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
//        //1.获取session
//        HttpSession session = request.getSession();
//        //2.获取session中的用户
//        Object user = session.getAttribute("user");
        //1.获取请求头中的token
        String token = request.getHeader("autoorization");
        if (StrUtil.isBlank(token)) {
            return true;
        }
        //2.基于token获取redis中的数据
        Map<Object, Object> user = stringRedisTemplate.opsForHash().entries(RedisConstants.LOGIN_CODE_KEY + token);
        //3.判断用户是否存在
        if (user == null) {
            //4.不存在,拦截
            return true;
        }
        // 将查询到的Hash数据转为UserDTO
        BeanUtil.fillBeanWithMap(user,new UserDTO(),false);
        //5.存在,保存用户信息到ThreadLocal
        UserHolder.saveUser((UserDTO) user);
        //刷新token有效期
        stringRedisTemplate.expire(RedisConstants.LOGIN_CODE_KEY + token,RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);
        //6.放行
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        //移除用户
        UserHolder.removeUser();
    }
}
