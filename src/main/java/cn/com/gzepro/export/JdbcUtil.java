package cn.com.gzepro.export;

/**
 * jdbc工具类
 */
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

class JdbcUtil {

    private static String driver = null;
    static String URL_GR = null;
    static String URL_EPRO = null;
    private static String username = null;
    private static String password = null;

    static{
        try {
            InputStream in = JdbcUtil.class.getClassLoader().getResourceAsStream("db.properties");
            Properties prop = new Properties();
            prop.load(in);

            driver = prop.getProperty("driver");
            URL_GR = prop.getProperty("urlGr");
            URL_EPRO = prop.getProperty("urlEpro");
            username = prop.getProperty("username");
            password = prop.getProperty("password");

            Class.forName(driver);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection(String url) throws SQLException{
        return DriverManager.getConnection(url, username,password);
    }

    public static void release(Connection conn,Statement st,ResultSet rs){

        if(rs!=null){
            try{
                rs.close();
            }catch (Exception e) {
                e.printStackTrace();
            }
            rs = null;

        }
        if(st!=null){
            try{
                st.close();
            }catch (Exception e) {
                e.printStackTrace();
            }

        }

        if(conn!=null){
            try{
                conn.close();
            }catch (Exception e) {
                e.printStackTrace();
            }

        }

    }
}
