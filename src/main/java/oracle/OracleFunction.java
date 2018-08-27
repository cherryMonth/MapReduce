package oracle;

import java.sql.*;

public class OracleFunction {

    public static void main(String [] args){
        try {
            // 1.加载驱动
            Class.forName("oracle.jdbc.driver.OracleDriver");
            // 2.得到连接
            Connection ct = DriverManager.getConnection(
                    "jdbc:oracle:thin:@192.168.1.15:1521:miku", "system", "Sj123456");
            // 3.创建PreparedStatement
            PreparedStatement ps = ct.prepareStatement("select get_pwd('12') result from dual");
            // 4.执行
            ResultSet rs=ps.executeQuery();
            if(rs.next()){
                Float annual=rs.getFloat("result");
                System.out.println(annual);
            }
            //5、关闭
            rs.close();
            ps.close();
            ct.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
