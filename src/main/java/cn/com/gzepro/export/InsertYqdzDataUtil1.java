package cn.com.gzepro.export;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static cn.com.gzepro.export.InsertDataUtil.PRE_NUM;
import static cn.com.gzepro.export.InsertDataUtil.SUF_NUM;
import static cn.com.gzepro.export.JdbcUtil.URL_EPRO;
import static cn.com.gzepro.export.JdbcUtil.URL_GR;

/**
 * 增量插入用气地址
 */
public class InsertYqdzDataUtil1 {
    /**
     * 获取的字段
     */
    private static final String FIELDS = "yqdz_id, yqdz_yqzh, yqdz_fwz_id, yqdz_kh_lx, yqdz_yqdzm_id," +
            "yqdz_yqdzm_ms, yqdz_dzlx, yqdz_fwjglx, yqdz_zt_id, yqdz_gastype, " +
            "org_id";

    static void insert() {
        Connection connGr = null;
        Connection connEpro = null;

        PreparedStatement stGr = null;
        PreparedStatement stEpro = null;

        ResultSet rsGr = null;

        try {
            connGr = JdbcUtil.getConnection(URL_GR);
            connEpro = JdbcUtil.getConnection(URL_EPRO);

            String sql = "select " + FIELDS + " from yongqidizhi where rownum <= " + SUF_NUM +
                    "and rownum >= " + PRE_NUM + " order by yqdz_yqzh desc";
            stGr = connGr.prepareStatement(sql);
            rsGr = stGr.executeQuery();

            String sql1 = "insert into yongqidizhi (" + FIELDS + ") values (?,?,?,?,?,?,?,?,?,?,?)";
            stEpro = connEpro.prepareStatement(sql1);

            // 遍历
            int i = 0;
            while(rsGr.next()) {
                    i++;

                    stEpro.setString(1, rsGr.getString(1));
                    stEpro.setString(2, rsGr.getString(2));
                    stEpro.setString(3, rsGr.getString(3));
                    stEpro.setString(4, rsGr.getString(4));
                    stEpro.setString(5, rsGr.getString(5));
                    stEpro.setString(6, " ");
                    stEpro.setString(7, " ");
                    stEpro.setString(8, " ");
                    stEpro.setString(9, " ");
                    stEpro.setString(10, " ");
                    stEpro.setString(11, " ");
                    stEpro.addBatch();

                    // 每1000次提交一次
                    if (i != 0 && i % 1000 == 0) {
                        stEpro.executeBatch();
                        connEpro.commit();
                        stEpro.clearBatch();
                    }

            }

            stEpro.executeBatch();
            connEpro.commit();
            stEpro.clearBatch();
            System.out.println("用气地址" + "插入成功");

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            JdbcUtil.release(connEpro, stEpro, null);
            JdbcUtil.release(connGr, stGr, rsGr);
        }
    }
}
