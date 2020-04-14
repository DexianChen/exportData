package cn.com.gzepro.export;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static cn.com.gzepro.export.InsertDataUtil.PRE_NUM;
import static cn.com.gzepro.export.InsertDataUtil.SUF_NUM;
import static cn.com.gzepro.export.JdbcUtil.URL_EPRO;
import static cn.com.gzepro.export.JdbcUtil.URL_GR;

/**
 * 插入工单数据
 */
class InsertWorkOrderDataUtil1 {
    /**
     * 获取的字段
     */
    private static final String FIELDS = "wo_id, type_id, status, priority, yqdz_id";

    static void insert() {
        Connection connGr = null;
        Connection connEpro = null;

        PreparedStatement stGr = null;
        PreparedStatement stEpro = null;

        ResultSet rsGr = null;

        try {
            connGr = JdbcUtil.getConnection(URL_GR);
            connEpro = JdbcUtil.getConnection(URL_EPRO);

            String sql = "select " + FIELDS + " from workorder where rownum <= " + SUF_NUM +
                    "and rownum >= " + PRE_NUM;
            stGr = connGr.prepareStatement(sql);
            rsGr = stGr.executeQuery();

            String sql1 = "insert into workorder (" + FIELDS + ") values (?,?,?,?,?)";
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
            System.out.println("工单表" + "插入成功");

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            JdbcUtil.release(connEpro, stEpro, null);
            JdbcUtil.release(connGr, stGr, rsGr);
        }
    }
}
