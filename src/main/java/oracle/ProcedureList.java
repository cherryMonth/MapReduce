package oracle;

import java.sql.*;
public class ProcedureList {
    public static void main(String[] args) {

        try {
            // 1.加载驱动
            Class.forName("oracle.jdbc.driver.OracleDriver");
            // 2.得到连接
            Connection ct = DriverManager.getConnection(
                    "jdbc:oracle:thin:@192.168.1.15:1521:miku", "system", "Sj123456");
            // 3.创建CallableStatement
            CallableStatement cs = ct.prepareCall("{call sp_proc(?,?)}");
            //给第一个?赋值
            cs.setInt(1,1);
            //给第二个?赋值
            cs.registerOutParameter(2,oracle.jdbc.OracleTypes.CURSOR);
            //4、执行
            cs.execute();
            //得到结果集
            ResultSet rs = (ResultSet) cs.getObject(2);
            while (rs.next()) {
                System.out.println(rs.getInt(1) + " " + rs.getString(2));
            }
            //5、关闭
            rs.close();
            cs.close();
            ct.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
