package tsinghua.edu;

import java.io.BufferedWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 将数据库中的数据导入到 HDFS 中
 * 目前直接转换成本地csv，使用hdfs客户端保存到hdfs中
 * 可考虑转换成 spark rdd 保存于 HDFS
 */
public class LoadData {



    public void dataTransfer(String prefixPath, BufferedWriter bufferedWriter, String url, String username, String pwd) throws Exception {

        Statement statement = getStatement(url, username, pwd);
        ResultSet res = statement.executeQuery("SELECT * FROM " + prefixPath);
        //get colume number
        ResultSetMetaData rsmd = res.getMetaData();
        int columnNum = rsmd.getColumnCount();
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < columnNum; i++)
            sb.append(rsmd.getColumnName(i) + ",");
        bufferedWriter.write(sb.toString().substring(0, sb.length() - 1) + "\n");

        int lineCount = 0;
        while (res.next()){
            lineCount++;
            List<String> rowitem = new ArrayList<>();
            for(int i = 0; i < columnNum; i++) {
                rowitem.add(res.getString(i));
            }
            // build string
            StringBuilder stringBuilder = new StringBuilder();
            for(String s : rowitem) {
                stringBuilder.append(s + ",");
            }
            bufferedWriter.write(stringBuilder.toString().substring(0, stringBuilder.length() - 1) + "\n");
        }
        System.out.println("Column Numbers:" + columnNum);
        System.out.println("Row Numbers:" + lineCount);
    }

    private Statement getStatement(String url, String username, String pwd) {
        Connection connection = null;
        Statement statement = null;

        try {
            Class.forName("cn.edu.tsinghua.iotdb.jdbc.TsfileDriver");
            connection = DriverManager.getConnection(url, username, pwd);
            statement = connection.createStatement();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return statement;
    }


}
