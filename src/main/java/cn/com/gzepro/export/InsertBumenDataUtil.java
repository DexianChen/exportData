package cn.com.gzepro.export;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static cn.com.gzepro.export.InsertDataUtil.PRE_NUM;
import static cn.com.gzepro.export.InsertDataUtil.SUF_NUM;
import static cn.com.gzepro.export.JdbcUtil.URL_EPRO;
import static cn.com.gzepro.export.JdbcUtil.URL_GR;

/**
 * 增量导入部门表数据
 */
class InsertBumenDataUtil {
    /**
     * 获取的字段
     */
    private static final String FIELDS = "bm_id, bm_code, org_id";

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

            String sql = "select " + FIELDS + " from bumen where rownum <= " + SUF_NUM +
                    "and rownum >= " + PRE_NUM;
            stGr = connGr.prepareStatement(sql);

            rsGr = stGr.executeQuery();

            String sql1 = "insert into bumen (" + FIELDS + ") values (?,?,?)";
            stEpro = connEpro.prepareStatement(sql1);

            String sql2 = "select " + FIELDS + " from bumen where bm_id = ? and rownum=1";
            stEpro1 = connEpro.prepareStatement(sql2);

            // 遍历
            int i = 0;
            while(rsGr.next()) {
                String bmId = rsGr.getString(1);
                stEpro1.setString(1, bmId);
                ResultSet resultSet = stEpro1.executeQuery();
                if (!resultSet.next()) {
                    i++;

                    stEpro.setString(1, rsGr.getString(1));
                    stEpro.setString(2, rsGr.getString(2));
                    stEpro.setString(3, rsGr.getString(3));
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
            System.out.println("部门表" + "插入成功");

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            JdbcUtil.release(connEpro, stEpro, null);
            JdbcUtil.release(connGr, stGr, rsGr);
        }
    }
}
