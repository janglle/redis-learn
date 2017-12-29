package com.mogoroom.redis;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @author 行一 at 2017/12/28 9:55
 */
public class Chapter01 {
    private static HostAndPort hnp = new HostAndPort("192.168.159.128", 6379);
    private static long ONE_WEEK_IN_SECONDS = 7 * 86400;
    private static long VOTE_SCORE = 432;

    public static void main(String... args) {
        JedisPool pool = new JedisPool(new JedisPoolConfig(), hnp.getHost(), hnp.getPort(), 2000);
        Jedis jedis = pool.getResource();
        Map<String, String> map = new HashMap<>();
        map.put("title", "gotostatement");
        map.put("link", "http://www.baidu.com");
        map.put("poster", "user:83271");
        long time = System.currentTimeMillis() / 1000;
        map.put("time", time + "");
        map.put("votes", "528");
        jedis.hmset("article:", map);
        jedis.zadd("time:", time, "article:1001");
        articleVote(jedis, "user:1", "article:1001");
    }

    public static void articleVote(Jedis jedis, String user, String article) {
        // 判断是否在投票期限内
        long cutOff = System.currentTimeMillis() / 1000 - ONE_WEEK_IN_SECONDS;
        Double publishTime = jedis.zscore("time:", article);
        if (publishTime == null || publishTime < cutOff) {
            return;
        }

        // 判断是否已投票
        int articleId = Integer.parseInt(article.split(":")[1]);
        Long sadd = jedis.sadd("voted:" + articleId, user);
        if (sadd != null && sadd == 1) {
            // 投票成功, 增加评分和文章评分次数
            jedis.hincrBy(article, "votes", 1);
            jedis.zincrby("score:", VOTE_SCORE, article);
        }
    }
}
