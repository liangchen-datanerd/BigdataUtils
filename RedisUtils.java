
import com.huawei.common.constants.PathCons;
import com.huawei.common.constants.RedisCons;
import com.huawei.jredis.client.GlobalConfig;
import com.huawei.jredis.client.auth.AuthConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisUtils {
    private final static Log LOG = LogFactory.getLog(RedisUtils.class.getName());

    private static RedisUtils redisUtils;
    //kerberos用户名
    private static String PRNCIPAL_NAME;
    private static String PATH_TO_KEYTAB = PathCons.PATH_TO_KEYTAB;
    private static String PATH_TO_KRB5_CONF = PathCons.PATH_TO_KRB5_CONF;

    private static JedisCluster client = null;

    private RedisUtils() {
    }


    public static RedisUtils getInstance(String kerberosUser) {
        PRNCIPAL_NAME = kerberosUser;
        if (redisUtils == null) {
            login();
            initClient();
            return new RedisUtils();
        } else {
            return redisUtils;
        }
    }

    private static void initClient() {
        Set<HostAndPort> hosts = new HashSet<HostAndPort>();
//        hosts.add(new HostAndPort(RedisCons.IP_1, RedisCons.PORT_1));
        hosts.add(new HostAndPort(RedisCons.IP_2, RedisCons.PORT_2));
//        hosts.add(new HostAndPort(RedisCons.IP_3, RedisCons.PORT_3));
        client = new JedisCluster(hosts, 5000);
    }

    private static void login() {

        String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;

        System.setProperty("java.security.krb5.conf", PATH_TO_KRB5_CONF);
        AuthConfiguration authConfiguration = new AuthConfiguration(PATH_TO_KEYTAB, PRNCIPAL_NAME);
        GlobalConfig.setAuthConfiguration(authConfiguration);

    }

    /**
     * 设置字符串类型
     *
     * @param key
     * @param value
     */
    public void setString(String key, String value) {
        client.set(key, value);
        LOG.info("key:" + key + " value: " + value + " has been set");
    }

    /**
     * 根据key获取值。不存在返回null
     *
     * @param key
     * @return
     */
    public String getString(String key) {
        return client.get(key);
    }

    /**
     * 追加字符串
     *
     * @param key
     * @param value 追加内容
     */
    public void appendString(String key, String value) {
        Long append = client.append(key, value);
        if (append == 0) {
            LOG.warn("key:" + key + " is not exist");
        } else {
            LOG.info("key:" + key + " has append value, After append value: " + client.get(key));
        }
    }

    /**
     * 删除key
     *
     * @param key
     */
    public void deleteKey(String key) {
        Long del = client.del(key);
        if (del == 0) {
            LOG.warn("key:" + key + " is not exist");
        } else {
            LOG.info("key:" + key + " has been delete");
        }
    }

    /**
     * 添加数据到List
     *
     * @param key
     * @param values
     */
    public void putDataList(String key, String... values) {
        Long rpush = client.rpush(key, values);
        if (rpush == 0) {
            LOG.warn("key:" + key + " is not exist");
        } else {
            LOG.info(values + " have been added to " + key);
        }
    }

    /**
     * 获取所有list数据
     *
     * @param key
     * @return
     */
    public List<String> getList(String key) {
        List<String> list = client.lrange(key, 0, -1);
        for (String s : list) {
            LOG.info("list data is " + s);
        }
        return list;
    }

    /**
     * 获取List类型数据的个数
     *
     * @param key
     * @return
     */
    public long getListCount(String key) {
        Long len = client.llen(key);
        LOG.info("Message count: " + len);
        return len;
    }

    /**
     * 创建Hash添加数据
     *
     * @param key
     * @param field
     * @param value
     */
    public void putDataHash(String key, String field, String value) {
        Long hset = client.hset(key, field, value);
        LOG.info("data field:" + field + " value:" + value + " have been added to " + key);
    }

    /**
     * 获取所有Hash值
     *
     * @param key
     * @return
     */
    public Map<String, String> getAllDataFromHash(String key) {
        Map<String, String> map = client.hgetAll(key);
        LOG.info("data are" + map);
        return map;
    }

    /**
     * 根据key获取Hash类型field值
     *
     * @param key
     * @param fieldName
     * @return
     */
    public String getFieldDataHash(String key, String fieldName) {
        String value = client.hget(key, "id");
        LOG.info("value:" + value);
        return value;
    }

    /**
     * 根据key获取Hash类型所有key
     *
     * @param key
     * @return
     */
    public Set<String> getHashKeys(String key) {
        Set<String> hkeys = client.hkeys(key);
        LOG.info(hkeys);
        return hkeys;
    }

    /**
     * 根据key获取Hash类型所有value
     *
     * @param key
     * @return
     */
    public List<String> getHashVals(String key) {
        List<String> hvals = client.hvals(key);
        LOG.info(hvals);
        return hvals;
    }

    /**
     * 为set添加数据
     * @param key
     * @param members
     */
    public void putDataSet(String key, String... members) {
        client.sadd(key, members);
        LOG.info("data have been added to " + key);
    }

    /**
     * 根据key获取set
     * @param key
     * @return
     */
    public Set<String> getDataSet(String key) {
        Set<String> sets = client.smembers(key);
        LOG.info("Set: " + sets);
        return sets;
    }

    /**
     * 给sortedSet类型添加数据
     * @param key
     * @param score 权重
     * @param value
     */
    public void putDataSortedSet(String key, double score, String value) {
        client.zadd(key, score, value);
        LOG.info("data " + value + " have been add to sortedset");
    }

    public Set<String> getAllDataSortedSet(String key) {
        Set<String> setValues = client.zrange(key, 0, -1);
        LOG.info("All values: " + setValues);
        return setValues;
    }

    /**
     * 为key设置过期时间
     *
     * @param key
     * @param seconds
     */
    public void setExpiredTime(String key, int seconds) {
        Long expire = client.expire(key, seconds);
        if (expire == 0) {
            LOG.warn("key:" + key + " is not exist");
        } else {
            LOG.info("key:" + key + " has been set expire time to " + seconds + " seconds");
        }
    }

}
