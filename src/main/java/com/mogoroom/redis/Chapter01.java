package com.mogoroom.redis;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ZParams;

import java.util.*;

/**
 * @author 行一 at 2017/12/28 9:55
 */
public class Chapter01 {
    private static HostAndPort hnp = new HostAndPort("192.168.159.128", 6379);
    private static int ONE_WEEK_IN_SECONDS = 7 * 86400;
    private static int VOTE_SCORE = 432;
    private static Jedis jedis;

    @Before
    public void init() {
        jedis = new Jedis(hnp.getHost(), hnp.getPort(), 2000);
        /*Map<String, String> map = new HashMap<>();
        map.put("title", "gotostatement");
        map.put("link", "http://www.baidu.com");
        map.put("poster", "user:83271");
        long time = System.currentTimeMillis() / 1000;
        map.put("time", time + "");
        map.put("votes", "528");
        jedis.hmset("article:1001", map);
        jedis.zadd("time:", time, "article:1001");
        articleVote(jedis, "user:1", "article:1001");*/
    }

    public void articleVote(Jedis jedis, String user, String article) {
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

    @Test
    public void postArticleTest() {
        postArticle("user:1", "learn redis", "http://www.baidu.com");
    }

    private long postArticle(String user, String title, String link) {
        Long articleId = jedis.incr("article:");
        // 初始化投票列表, 将自己保存到投票列表, 并设置过期时间
        String voted = "voted:" + articleId;
        jedis.sadd(voted, user);
        jedis.expire(voted, ONE_WEEK_IN_SECONDS);

        // 保存文章, 记录到时间和评分排序列表
        Map<String, String> articleMap = new HashMap<>();
        articleMap.put("title", title);
        articleMap.put("link", link);
        articleMap.put("poster", user);
        long now = System.currentTimeMillis() / 1000;
        articleMap.put("time", now + "");
        articleMap.put("votes", "1");
        String article = "article:" + articleId;
        jedis.hmset(article, articleMap);
        jedis.zadd("time:", now, article);
        jedis.zadd("score:", now + VOTE_SCORE, article);
        return articleId;
    }

    @Test
    public void getArticlesTest() {
        List<Map<String, String>> articles = getArticles(0, "score:");
        System.out.println(articles);
    }

    private static final int ARTICLES_PER_PAGE = 25;

    private List<Map<String, String>> getArticles(int page, String key) {
        int start = (page - 1) * ARTICLES_PER_PAGE;
        int end = start + ARTICLES_PER_PAGE - 1;
        Set<String> articleSet = jedis.zrevrange(key, start, end);
        List<Map<String, String>> articles = new ArrayList<>();
        for (String article : articleSet) {
            Map<String, String> articleMap = jedis.hgetAll(article);
            articleMap.put("id", article);
            articles.add(articleMap);
        }
        return articles;
    }

    @Test
    public void addRemoveGroupsTest() {
        addRemoveGroups(1, new String[]{"programming", "study"}, new String[]{});
    }

    private void addRemoveGroups(int articleId, String[] toAdd, String[] toRemove) {
        String article = "article:" + articleId;
        for (String group : toAdd) {
            jedis.sadd("group:" + group, article);
        }
        for (String group : toRemove) {
            jedis.srem("group:" + group, article);
        }
    }

    @Test
    public void getGroupArticlesTest() {
        List<Map<String, String>> programming = getGroupArticles("programming", 0, "score:");
        System.out.println(programming);
    }

    private List<Map<String, String>> getGroupArticles(String group, int page, String key) {
        String interSetKey = key + group;
        if (Boolean.TRUE != jedis.exists(interSetKey)) {
            jedis.zinterstore(interSetKey, new ZParams().aggregate(ZParams.Aggregate.MAX), "group:" + group, key);
            jedis.expire(interSetKey, 60);
        }
        return getArticles(page, interSetKey);
    }

    @Test
    public void testAll() {

    }
}
