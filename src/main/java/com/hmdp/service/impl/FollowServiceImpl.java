package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.BeanUtils;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IUserService userService;
    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        //获取当前登陆的用户id
        Long userId = UserHolder.getUser().getId();
        //判断到底是关注还是取关
        if (isFollow) {
            //关注,新增数据
            Follow follow = new Follow();
            follow.setFollowUserId(followUserId);
            follow.setUserId(userId);
            boolean save = save(follow);
            if (save) {
                //把关注用户的id,放入redis的set集合里 sadd userId followUserId
                stringRedisTemplate.opsForSet().add("follows:" + userId,followUserId.toString());
            }
        }else {
            //取关,删除 delete from tb_follow where user_id = ? and follow_user_id = ?
            remove(new QueryWrapper<Follow>()  //创建一个QueryWrapper对象的语法，QueryWrapper一个查询构造器，它可以用来构建查询条件。在这里，<Follow>指定了要操作的实体类类型为Follow
                    .eq("user_id",userId).eq("follow_user_id",followUserId));
        }
        return Result.ok();
    }

    @Override
    public Result isFllow(Long followUserId) {
        //获取登陆用户id
        Long userId = UserHolder.getUser().getId();
        //查询是否关注 select count(*) from tb_follow where user_id = ? and follow_user_id = ?
        query().eq("user_id",userId).eq("follow_user_id",followUserId).count();
        //判断
        return Result.ok();
    }

    @Override
    public Result followComments(Long id) {
        //1.获取当前用户
        Long userId = UserHolder.getUser().getId();
        //2.求交集
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect/*是一个集合Set,包含了两个指定集合的交集元素*/("follows:" + userId, "follows:" + id);
        if (intersect == null || intersect.isEmpty()) {
            //无交集
            return Result.ok(Collections.emptyList());
        }
        //3.解析id集合
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        //4.查询用户
        Stream<UserDTO> userDTOStream = userService.listByIds(ids)
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class));
        return Result.ok(userDTOStream);
    }
}
