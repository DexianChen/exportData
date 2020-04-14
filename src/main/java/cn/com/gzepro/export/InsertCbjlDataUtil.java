package cn.com.gzepro.export;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static cn.com.gzepro.export.InsertDataUtil.*;
import static cn.com.gzepro.export.JdbcUtil.URL_EPRO;
import static cn.com.gzepro.export.JdbcUtil.URL_GR;

/**
 * 增量插入工单数据
 */
class InsertCbjlDataUtil {
    /**
     * 获取的字段
     */
    private static final String FIELDS = "cbjl_id, org_id, cbjl_yqdz, cbjl_yqzh, cbjl_bcyql_js";

    static void insert() {
        Connection connGr = null;
        Connection connEpro = null;

        PreparedStatement stGr = null;
        PreparedStatement stEpro = null;
        PreparedStatement stEpro1 = null;

        ResultSet rsGr = null;

        try {
            connGr = JdbcUtil.getConnection(URL_GR);
            connEpro = JdbcUtil.getConnection(URL_EPRO);

            String sql = "select " + FIELDS + " from chaobiao_jilu where rownum <= " + SUF_NUM +
                    "and rownum >= " + PRE_NUM;
            stGr = connGr.prepareStatement(sql);
            rsGr = stGr.executeQuery();

            String sql1 = "insert into chaobiao_jilu (" + FIELDS + ") values (?,?,?,?,?)";
            stEpro = connEpro.prepareStatement(sql1);

            String sql2 = "select " + FIELDS + " from chaobiao_jilu where cbjl_id = ? and rownum=1";
            stEpro1 = connEpro.prepareStatement(sql2);

            // 遍历
            int i = 0;
            while(rsGr.next()) {
                String cbjlId = rsGr.getString(1);
                stEpro1.setString(1, cbjlId);
                ResultSet resultSet = stEpro1.executeQuery();
                if (!resultSet.next()) {
                    i++;

                    stEpro.setString(1, rsGr.getString(1));
                    stEpro.setString(2, rsGr.getString(2));
                    stEpro.setString(3, rsGr.getString(3));
                    stEpro.setString(4, rsGr.getString(4));
                    stEpro.setBigDecimal(5, rsGr.getBigDecimal(5));
                    stEpro.addBatch();

                    // 每1000次提交一次
                    if (i != 0 && i % 1000 == 0) {
                        stEpro.executeBatch();
                        connEpro.commit();
                        stEpro.clearBatch();
                    }
                }
            }

            stEpro.executeBatch();
            connEpro.commit();
            stEpro.clearBatch();
            System.out.println("抄表记录" + "插入成功");

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            JdbcUtil.release(connEpro, stEpro1, null);
            JdbcUtil.release(connEpro, stEpro, null);
            JdbcUtil.release(connGr, stGr, rsGr);
        }
    }
}
