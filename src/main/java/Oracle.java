import java.sql.*;

public class Oracle {
    public static void main(String [] args){
        try {
            // 1.加载驱动
            Class.forName("oracle.jdbc.driver.OracleDriver");
            // 2.得到连接
            Connection ct = DriverManager.getConnection(
                    "jdbc:oracle:thin:@192.168.1.15:1521:miku", "system", "Sj123456");

            // 3.创建CallableStatement
            CallableStatement cs = ct.prepareCall("{call pwd_update(?,?)}");
            // 4.给?赋值
            cs.setString(1, "12");
            cs.setInt(2, 123123312);
            // 5.执行
            cs.execute();
            // 关闭
            cs.close();
            ct.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
