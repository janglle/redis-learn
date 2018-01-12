package com.mogoroom.redis;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author 行一 at 2018/1/4 9:54
 */
public class Chapter02 {
    private static HostAndPort hnp = new HostAndPort("192.168.159.128", 6379);
    private static Jedis jedis;

    @Before
    public void init() {
        jedis = new Jedis(hnp.getHost(), hnp.getPort(), 2000);
    }

    public String checkToken(String token) {
        return jedis.hget("login:", token);
    }

    public void updateToke(String token, String user, String item) {
        jedis.hset("login:", token, user);
        long timestamp = System.currentTimeMillis();
        jedis.zadd("recent:", timestamp, token);
        if (item != null) {
            jedis.zadd("viewed:" + token, timestamp, item);
            jedis.zremrangeByRank("viewed:" + token, 0, -26);
        }
    }

    boolean quite = false;
    int limit = 1000_0000;

    public void cleanSessions() throws InterruptedException {
        while (!quite) {
            Long size = jedis.zcard("recent:");
            if (size < limit) {
                Thread.sleep(1);
                continue;
            }
            int endIndex = (int) Math.min(size - limit, 100);
            Set<String> tokens = jedis.zrange("recent:", 0, endIndex - 1);
            List<String> sessionKeys = new ArrayList<>(tokens.size());
            tokens.forEach(token -> sessionKeys.add("viewed:" + token));
            jedis.del(sessionKeys.toArray(new String[sessionKeys.size()]));
            jedis.hdel("login:", tokens.toArray(new String[tokens.size()]));
            jedis.zrem("recent:", tokens.toArray(new String[tokens.size()]));
        }
    }

    @Test
    public void tt() {
        String s = "sh.h5.sdk";
        System.out.println(s.substring("sh.".length()));
    }

}
