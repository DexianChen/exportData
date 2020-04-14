package cn.com.gzepro.export;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static cn.com.gzepro.export.InsertDataUtil.*;
import static cn.com.gzepro.export.JdbcUtil.URL_EPRO;
import static cn.com.gzepro.export.JdbcUtil.URL_GR;

/**
 * 插入部门
 */
public class InsertRqbDataUtil {
    /**
     * 获取的字段
     */
    private static final String FIELDS = "rqb_id, rqb_yqdz_id, rqb_zt, rqb_wz_id, rqb_kssyrq," +
            "rqb_synx, rqb_tzxs, rqb_gdfs, rqb_bjzz, rqb_csbd," +
            "rqb_bds, org_id, rqb_rqblx_id";

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

            String sql = "select " + FIELDS + " from ranqibiaowhere rownum <= " + SUF_NUM +
                    "and rownum >= " + PRE_NUM;
            stGr = connGr.prepareStatement(sql);
            rsGr = stGr.executeQuery();

            String sql1 = "insert into ranqibiao (" + FIELDS + ") values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            stEpro = connEpro.prepareStatement(sql1);

            String sql2 = "select " + FIELDS + " from ranqibiao where rqb_id = ? and rownum=1";
            stEpro1 = connEpro.prepareStatement(sql2);

            // 遍历
            int i = 0;
            while(rsGr.next()) {
                String rqbId = rsGr.getString(1);
                stEpro1.setString(1, rqbId);
                ResultSet resultSet = stEpro1.executeQuery();
                if (!resultSet.next()) {
                    i++;

                    stEpro.setString(1, rsGr.getString(1));
                    stEpro.setString(2, rsGr.getString(2));
                    stEpro.setString(3, rsGr.getString(3));
                    stEpro.setString(4, rsGr.getString(4));
                    stEpro.setString(1, rsGr.getString(5));
                    stEpro.setString(2, rsGr.getString(6));
                    stEpro.setString(3, rsGr.getString(7));
                    stEpro.setString(4, rsGr.getString(8));
                    stEpro.setString(1, rsGr.getString(9));
                    stEpro.setString(2, rsGr.getString(10));
                    stEpro.setString(3, rsGr.getString(11));
                    stEpro.setString(4, rsGr.getString(12));
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
            System.out.println("燃气表" + "插入成功");

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            JdbcUtil.release(connEpro, stEpro1, null);
            JdbcUtil.release(connEpro, stEpro, null);
            JdbcUtil.release(connGr, stGr, rsGr);
        }
    }
}
