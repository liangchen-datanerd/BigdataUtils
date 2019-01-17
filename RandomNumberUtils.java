

import java.util.Random;

/**
 * 随机生产数字字符串
 */
public class RandomNumberUtils {

    /**
     * 返回手机号码
     */
    private static String[] telFirst = "134,135,136,137,138,139,150,151,152,157,158,159,130,131,132,155,156,133,153".split(",");

    public static String getTel() {

        Random random = new Random();//定义random，产生随机数
        String number = telFirst[random.nextInt(telFirst.length)];//定义电话号码以139开头
        for (int j = 0; j < 8; j++) {
            //生成0~9 随机数
            number += random.nextInt(9);
        }
        return number;
    }

}
