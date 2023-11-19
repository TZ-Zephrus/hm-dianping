package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import jakarta.servlet.http.HttpSession;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;


import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_CODE_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_TOKEN_KEY;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Autowired
    public StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        // 1.校验手机号
        if (RegexUtils.isPhoneInvalid(phone))
            // 2.如果不符合返回错误信息
            return Result.fail("手机号格式不正确");


        // 3.符合，生成验证码
        String code = RandomUtil.randomNumbers(6);
        // 4.保存验证码到redis
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY+phone, code, 2, TimeUnit.MINUTES);

        // 5.发送验证码
        log.debug("发送短信验证码成功，验证码: {}", code);
        // 返回ok
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //1.校验手机号
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone))
            // 2.如果不符合返回错误信息
            return Result.fail("手机号格式不正确");
        //3.从redis获取验证码，校验验证码
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if (cacheCode == null || !cacheCode.equals(code)) {
            //3.不一致报错
            return Result.fail("不一致报错");
        }

        //4.一致 根据手机号查用户
        User user = query().eq("phone", phone).one();

        //5,判断用户是否存在
        if (user == null) {
            //6.不存在，创建用户
            user = createUserWithPhone(phone);
        }

        //7.保存用户信息进redis
        //7.1.随机生成token，作为登陆令牌
        String token = UUID.randomUUID().toString();
        //7.2.将user对象转为Hash存储
        UserDTO userDTO = new UserDTO();
        BeanUtil.copyProperties(user, userDTO);
        //转换类型。因为userDTO中有一项为long，用StringRedisTemplate会出错
        Map<String, Object> map = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create().setIgnoreNullValue(true).setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));
        //使用putAll,需要将userDTO转变为map
        //7.3.进行存储  设置一个有效期
        String tokenKey = LOGIN_TOKEN_KEY+token;
        stringRedisTemplate.opsForHash().putAll(tokenKey, map);
        //7.4.设置有效期
        stringRedisTemplate.expire(tokenKey, 30, TimeUnit.MINUTES);
        //8.返回token
        return Result.ok(token);
    }

    private User createUserWithPhone(String phone) {
        // 1.创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        // 2.保存用户
        save(user);
        return user;
    }
}
