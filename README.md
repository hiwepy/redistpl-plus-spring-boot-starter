# redistpl-plus-spring-boot-starter

Extension of Spring Data Redis

### 组件简介

> 基于 Spring Data Redis 的 扩展 Spring Boot Starter 实现

### 使用说明

##### 1、Spring Boot 项目添加 Maven 依赖

``` xml
<dependency>
	<groupId>com.github.hiwepy</groupId>
	<artifactId>redistpl-plus-spring-boot-starter</artifactId>
	<version>2.0.0.RELEASE</version>
</dependency>
```

##### 2、在`application.yml`文件中增加如下配置

```yaml
spring:
  # Redis相关配置
  redis:
    # Redis服务器地址
    host: 127.0.0.1
    # Redis服务器连接端口
    port: 6379
    # Redis服务器连接密码（默认为空）
    password: 132456
    # 基于Lettuce客户端的Redis连接池配置
    lettuce:
      pool:
        # 连接池最大连接数（使用负值表示没有限制）
        max-active: 200
        # 连接池最大阻塞等待时间（使用负值表示没有限制）
        max-wait: -1
        # 连接池中的最大空闲连接
        max-idle: 10
        # 连接池中的最小空闲连接
        min-idle: 0
    # 连接超时时间（毫秒）
    timeout: 5000
    # 设置消息监听容器线程池
    executor:
      subscription:
        pool:
          max-idle: 8
      listener:
        pool:
          max-idle: 8

```

##### 3、使用示例

```java

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.redis.core.RedisOperationTemplate;

@SpringBootApplication
public class RedisApplication_Test {

    @Autowired
    private RedisOperationTemplate template;

    @PostConstruct
    public void test() {

        Long val = template.getLong("xxx");
        Long val2 = template.getLong("xxx", 0L);

    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(RedisApplication_Test.class, args);
    }

}
```

## Jeebiz 技术社区

Jeebiz 技术社区 **微信公共号**、**小程序**，欢迎关注反馈意见和一起交流，关注公众号回复「Jeebiz」拉你入群。

|公共号|小程序|
|---|---|
| ![](https://raw.githubusercontent.com/hiwepy/static/main/images/qrcode_for_gh_1d965ea2dfd1_344.jpg)| ![](https://raw.githubusercontent.com/hiwepy/static/main/images/gh_09d7d00da63e_344.jpg)|

