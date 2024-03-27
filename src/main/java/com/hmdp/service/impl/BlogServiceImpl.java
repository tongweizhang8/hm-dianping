package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IUserService userService;
    @Resource
    private IFollowService followService;

    @Override
    public Result queryHotBlog(Integer current) {
        //根据用户查询
        Page<Blog> liked = query().orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        //获取当前页数据
        List<Blog> records = liked.getRecords();
        //查询用户
        records.forEach(blog -> { //Lambda表达式，其结构为 (参数) -> { 方法体 },遍历一个记录集
            this.queryBlogUser(blog);
            this.isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User byId = userService.getById(userId);
        blog.setName(byId.getNickName());
        blog.setIcon(byId.getIcon());
    }

    @Override
    public Result queryBlogById(Long id) {
        //1.查询blog
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("笔记不存在");
        }
        //2.查询blog有关用户
        queryBlogUser(blog);
        //3.查询blog是否被点赞
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        Long userId = UserHolder.getUser().getId();
        Double member = stringRedisTemplate.opsForZSet().score("blog::liked" + blog.getId(), userId.toString()); //opsForZSet用于操作有序集合
        blog.setIsLike(member != null);
    }

    @Override
    public Result likeBiog(Long id) {
        //获取登陆用户
        Long userId = UserHolder.getUser().getId();
        //判断当前登陆用户是否已经点赞
        Double score= stringRedisTemplate.opsForZSet().score("blog:liked" + id, userId.toString());
        if (score == null) { //判断当前用户是否已经点赞了某篇博客
            //如果未点赞,可以点赞
            //数据库点赞数+1
            boolean update = update().setSql("like = like + 1").eq("id", id).update();
            //保存用户到redis的set集合
            if (update) { //检查之前的更新操作是否成功执行
                stringRedisTemplate.opsForZSet().add("blog:liked" + id, userId.toString(),System.currentTimeMillis());
            }
        } else {
            //如果已经点赞,取消点赞
            //数据库点赞数-1
            boolean update = update().setSql("like = like - 1").eq("id", id).update();
            //把用户从redis的set集合中移除
            if (update) {
                stringRedisTemplate.opsForZSet().remove("blog:liked" + id,userId.toString());
            }
        }
        return null;
    }

    @Override
    public Result queryBlogLikes(Long id) {
        //查询top5点赞数目
        Set<String> top = stringRedisTemplate.opsForZSet().range(RedisConstants.BLOG_LIKED_KEY + id, 0, 4);
        if (top == null || top.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        //解析出其中的用户id
        List<Long> collect = top.stream().map(Long::valueOf/*将每个元素从字符串类型转换为Long类型*/).collect(Collectors.toList());
        //根据用户id查询用户
        List<UserDTO> userDTOS = userService.listByIds(collect)
                .stream() //将返回的用户对象列表转换为一个流
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList()); //将流中的元素收集到一个列表
        //返回
        return Result.ok(userDTOS);
    }

    @Override
    public Result saveBlog(Blog blog) {
        //1.获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        //2.保存探店笔记
        boolean save = save(blog);
        if (save) {
            return Result.fail("新增失败");
        }
        //3.查询笔记作者的所有粉丝 select * from tb_follow where follow_user_id = ?
        List<Follow> followUserId = followService.query() //返回一个用于构建查询条件的 QueryWrapper 对象
                .eq("follow_user_id", user.getId()).list(); //调用 list() 方法执行查询操作
        //4.推送笔记id给所有粉丝
        stringRedisTemplate.opsForZSet() //获取了一个操作有序集合的对象
                .add("feed:" + followUserId,blog.getId().toString(),System.currentTimeMillis()); //add(key, value, score) 方法将指定的值（value）添加到有序集合中，并指定一个分数（score）
        //3.返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offSet) {
        //1.获取当前id
        Long id = UserHolder.getUser().getId();
        //2.查询收件箱 ZREVRANDGEBYSCORE key Max Min LIMIT offSet count
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet().reverseRangeByScoreWithScores(RedisConstants.FEED_KEY + id, 0, max, offSet, 2);
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }
        //3.解析数据:blogId,minTime(时间戳),offSet
        //获取最小时间和offset
        ArrayList<Object> ids = new ArrayList<>();
        long minTime = 0;
        int os = 1;
        for (ZSetOperations.TypedTuple<String> tuple : typedTuples) {
            //获取id
            ids.add(tuple.getValue());
            //获取分数(时间戳)
            long time = tuple.getScore().longValue(); //longValue()方法用于将其转换为基本的long类型数据
            if (time == minTime) {
                os++;
            }else {
                minTime = time;
                os = 1;
            }
        }
        //4.根据id查询blog
        String idstr = StrUtil.join("," + ids); //将一个数组或集合中的元素连接成一个字符串
        List<Blog> list = query().in("ids", id).last("ORDER BY FIELD(id," + idstr + ")").list();//last()方法用于在SQL查询语句的末尾添加自定义的SQL片段
        for (Blog blog : list) {
            //查询blog有关的用户
            queryBlogUser(blog);
            //查询blog是否被点赞
            isBlogLiked(blog);
        }
        //5.封装并返回
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(list);
        scrollResult.setOffset(offSet);
        scrollResult.setMinTime(minTime);
        return Result.ok(scrollResult);
    }
}
