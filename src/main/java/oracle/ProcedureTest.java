package oracle;

import java.sql.*;

public class ProcedureTest {
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
            cs.registerOutParameter(2,oracle.jdbc.OracleTypes.VARCHAR);
            //4、执行
            cs.execute();
            //取出返回值,要注意？的顺序
            String name=cs.getString(2);
            System.out.println("编号1的名字："+name);
            //5、关闭
            cs.close();
            ct.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
