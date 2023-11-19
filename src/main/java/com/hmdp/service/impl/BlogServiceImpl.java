package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.controller.BlogController;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;

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

    @Autowired
    private IUserService userService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private IFollowService followService;

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.queryBlogUser(blog);
            this.isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
        // 1.查询blog
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("笔记不存在！");
        }
        // 2.查询blog有关的用户
        queryBlogUser(blog);
        // 3.查询blog是否被点赞
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        // 1.获取登录用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            // 用户未登录，无需查询是否点赞
            return;
        }
        Long userId = user.getId();
        // 2.判断当前登录用户是否已经点赞
        String key = "blog:liked:" + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }

    @Override
    public Result likeBlog(Long id) {
        // 1.获取登录用户
        Long userId = UserHolder.getUser().getId();
        // 2.判断当前登录用户是否已经点赞
        String key = BLOG_LIKED_KEY + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if (score == null) {
            // 3.如果未点赞，可以点赞
            // 3.1.数据库点赞数 + 1
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            // 3.2.保存用户到Redis的set集合  zadd key value score
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        } else {
            // 4.如果已点赞，取消点赞
            // 4.1.数据库点赞数 -1
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            // 4.2.把用户从Redis的set集合移除
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        String key = BLOG_LIKED_KEY + id;
        //1 查询top5的点赞用户  zrange key 0 4;
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if (top5 == null || top5.isEmpty()) {
            return Result.ok();
        }
        //2 解析出用户id
        List<Long> list = top5.stream().map(Long::valueOf).toList();
        //3 根据用户id查询用户   手动指定顺序
        String join = StrUtil.join(",", list);
        LambdaQueryWrapper<User> lambdaQueryWrapper = new LambdaQueryWrapper<>();
        lambdaQueryWrapper.in(User::getId, list).last("order by field(id,"+join+")");
        List<User> users = userService.list(lambdaQueryWrapper);
        //4 改为userDTO
        List<UserDTO> list1 = users.stream().map(user -> {
            UserDTO userDTO = new UserDTO();
            BeanUtil.copyProperties(user, userDTO);
            return userDTO;
        }).toList();
        //4 返回
        return Result.ok(list1);
    }

    @Override
    public Result saveBlog(Blog blog) {
        //1. 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId((user.getId()));
        //2. 保存探店笔记
        boolean isSuccess = save(blog);
        if (!isSuccess) {
            Result.fail("新增笔记失败");
        }
        //3. 查询笔记作者的所有粉丝
        List<Follow> followUserId = followService.query().eq("follow_user_id", user.getId()).list();
        //4.推送笔记id给所有分析
        for (Follow follow : followUserId) {
            //4.1 获取粉丝id
            Long userId = follow.getUserId();
            //4.2 推送
            String key = "feed:" + userId;
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        //1 获取当前用户
        Long userId = UserHolder.getUser().getId();
        //2 查询收件箱 ZREVRABGEBTSCORE key Max Min LIMIT offset count
        String key = RedisConstants.FEED_KEY + userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 3);
        //3 非空判断
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }
        //4 解析数据 blogId minTime offset(与上次查找值一样的元素)
        List<Long> blogIds = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        int count = 1;    //用于计数最小时间戳个数
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            // 4.1 获取blogi=Id
            String blogIdStr = typedTuple.getValue();
            blogIds.add(Long.valueOf(blogIdStr));
            // 4.2 获取分数时间戳
            long time = typedTuple.getScore().longValue();
            if (time == minTime) {
                count++;
            } else {
                minTime = typedTuple.getScore().longValue();
                count = 1;
            }
        }
        //5 根据id查询blog
        String join = StrUtil.join(",", blogIds);
        LambdaQueryWrapper<Blog> lambdaQueryWrapper = new LambdaQueryWrapper<>();
        lambdaQueryWrapper.in(Blog::getId, blogIds).last("order by field(id,"+join+")");
        List<Blog> blogs = this.list(lambdaQueryWrapper);

        for (Blog blog : blogs) {
            //查询blog有关用户
            queryBlogUser(blog);
            //查询blog是否被点赞
            isBlogLiked(blog);
        }
        //6 封装并返回
        ScrollResult r = new ScrollResult();
        r.setList(blogs);
        r.setMinTime(minTime);
        r.setOffset(count);

        return Result.ok(r);
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
