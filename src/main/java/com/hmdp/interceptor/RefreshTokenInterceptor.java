package com.hmdp.interceptor;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.UserDTO;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.UserHolder;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RefreshTokenInterceptor implements HandlerInterceptor {

    //这里只能使用构造函数注入
    //因为这里的对象Interceptor是我们手动new出来的，不是spring帮我们创建的，他不能帮我们做依赖注入！
    //加了@Configuration说明是Spring创建的对象
    private StringRedisTemplate stringRedisTemplate;

    public RefreshTokenInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
//        // 1.判断是否需要拦截（ThreadLocal中是否有用户）
//        if (UserHolder.getUser() == null) {
//            // 没有，需要拦截，设置状态码
//            response.setStatus(401);
//            // 拦截
//            return false;
//        }
//        // 有用户，则放行
//        return true;

        //1.获取请求头中的token
        String token = request.getHeader("authorization");
        if (!StringUtils.hasText(token)) {
            //不存在 拦截
//            response.setStatus(401);
            return true;
        }
        //2.基于TOKEN获取redis中的用户
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash()
                .entries(RedisConstants.LOGIN_TOKEN_KEY+token);
        // 3.判断用户是否存在
        if (userMap.isEmpty()) {
            //4.不存在，拦截,―返回401状态码
//            response.setStatus(401);
            return true;
        }
        //5.将查询到的Hash数据转为UserDTO对象
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
        //6.存在，保存用户信息到ThreadLocal
        UserHolder.saveUser(userDTO);
        //7.刷新token有效期
        stringRedisTemplate.expire(RedisConstants.LOGIN_TOKEN_KEY+token, 30, TimeUnit.MINUTES);
        // 8.放行
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        UserHolder.removeUser();
    }
}
