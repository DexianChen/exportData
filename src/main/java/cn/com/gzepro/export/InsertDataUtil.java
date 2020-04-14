package cn.com.gzepro.export;

import java.io.InputStream;
import java.util.Properties;

/**
 * 插入数据总入口
 */
public class InsertDataUtil {
    /**
     * 获取的记录数
     */
    static Integer PRE_NUM = null;
    static Integer SUF_NUM = null;

    static{
        try {
            InputStream in = JdbcUtil.class.getClassLoader().getResourceAsStream("db.properties");
            Properties prop = new Properties();
            prop.load(in);

            PRE_NUM = Integer.parseInt(prop.getProperty("preNum"));
            SUF_NUM = Integer.parseInt(prop.getProperty("sufNum"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        InsertFyjlDataUtil.insert("feiyong_jilu_ys");
//        InsertFyjlDataUtil.insert("feiyong_jilu_ss");
        InsertYqdzDataUtil1.insert();
        InsertWorkOrderDataUtil1.insert();
        InsertCbjlDataUtil1.insert();
//        InsertFyjlDataUtil1.insert("feiyong_jilu_ss");
    }
}
