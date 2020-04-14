package cn.com.gzepro.export;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static cn.com.gzepro.export.InsertDataUtil.PRE_NUM;
import static cn.com.gzepro.export.InsertDataUtil.SUF_NUM;
import static cn.com.gzepro.export.JdbcUtil.URL_EPRO;
import static cn.com.gzepro.export.JdbcUtil.URL_GR;

/**
 * 插入应收表或实收表数据工具类
 */
class InsertFyjlDataUtil1 {
    /**
     * 获取的字段
     */
    private static final String FIELDS = "fyjl_id, fyjl_bi_id, fyjl_fylx_id, fyjl_yqzh, fyjl_yje, " +
            "fyjl_sjjfe, fyjl_Jfjzrq, fyjl_Znj_Bl, fyjl_Zt, fyjl_Mian_Znj, " +
            "fyjl_Znj_Jmcs, fyjl_sjjfrq, org_id";

    static void insert(String tableName) {
        Connection connGr = null;
        Connection connEpro = null;

        PreparedStatement stGr = null;
        PreparedStatement stEpro = null;

        ResultSet rsGr = null;

        try {
            connGr = JdbcUtil.getConnection(URL_GR);
            connEpro = JdbcUtil.getConnection(URL_EPRO);

            String sql = "select " + FIELDS + " from " + tableName + " where rownum <= " + SUF_NUM +
                    "and rownum >= " + PRE_NUM;
            stGr = connGr.prepareStatement(sql);
            rsGr = stGr.executeQuery();

            String sql1 = "insert into " + tableName + " (" + FIELDS + ") values (?,?,?,?,?,?,?,?,?,?,?,?,?)";
            stEpro = connEpro.prepareStatement(sql1);

            // 遍历
            int i = 0;
            while(rsGr.next()) {
                    i++;

                    stEpro.setString(1, rsGr.getString(1));
                    stEpro.setString(2, rsGr.getString(2));
                    stEpro.setString(3, rsGr.getString(3));
                    stEpro.setString(4, rsGr.getString(4));
                    stEpro.setBigDecimal(5, rsGr.getBigDecimal(5));
                    stEpro.setBigDecimal(6, rsGr.getBigDecimal(6));
                    stEpro.setDate(7, rsGr.getDate(7));
                    stEpro.setBigDecimal(8, rsGr.getBigDecimal(8));
                    stEpro.setString(9, rsGr.getString(9));
                    stEpro.setString(10, rsGr.getString(10));
                    stEpro.setBigDecimal(11, rsGr.getBigDecimal(11));
                    stEpro.setDate(12, rsGr.getDate(12));
                    stEpro.setString(13, rsGr.getString(13));
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
            System.out.println(tableName + "插入成功");

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            JdbcUtil.release(connEpro, stEpro, null);
            JdbcUtil.release(connGr, stGr, rsGr);
        }
    }
}
