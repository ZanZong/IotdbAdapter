package tsinghua.edu;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * 将拿到的原始数据导入到数据库，暂时不做更多参数设置，如有必要，可完善为单独的功能
 * INSERT INTO root.yanmoji.shenzhen.d1(timestamp,
 * axis1pos, axis2pos, axis3pos, axis4pos, axis1vel,
 * axis2vel, axis3vel, axis4vel) VALUES
 * (2017-08-05T12:37:39.929+08:00,-61.9,-12.0,-20.8,88.2,0.0,-0.1,0.0,0.0);
 */
public class LoadData2IotDB {

    void loadDataForTest() throws SQLException {

        Map<Integer, List<String>> data = new HashMap<>();
        int count = 0;
        try {
            File csv = new File("/home/zongzan/dump.csv"); // CSV文件

            BufferedReader br = new BufferedReader(new FileReader(csv));
            String line = "";
            while ((line = br.readLine()) != null) {
                // set ',' as split tag for each line
                StringTokenizer st = new StringTokenizer(line, ",");
                List<String> items = new ArrayList<>();
                while (st.hasMoreTokens()) {
                    items.add(st.nextToken());
                }
                data.put(count++, items);
            }
            br.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Load data line num:" + count);
        Map<Integer, String> DEBUG_DATA = new HashMap<>();

        Connection connection = null;
        Statement statement = null;
        try {
            Class.forName("cn.edu.tsinghua.iotdb.jdbc.TsfileDriver");
            connection = DriverManager.getConnection("jdbc:tsfile://localhost:6667/", "root", "root");
            statement = connection.createStatement();
            for (int i = 0; i < count; i++){
                List<String> items = data.get(i);
                StringBuilder stringBuilder = new StringBuilder("");
                // build the sql sting, and delete last ','
                for(String s : items){
                    stringBuilder.append(s);
                    stringBuilder.append(',');
                }
                String ts = stringBuilder.toString().substring(0, stringBuilder.length() - 1);
                String sql = "INSERT INTO root.yanmoji.shenzhen.d1(timestamp," +
                        "axis1pos, axis2pos, axis3pos, axis4pos, axis1vel,axis2vel, axis3vel, axis4vel, axis1torque, " +
                        "axis2torque, axis3torque, axis4torque, axis1set, axis2set, axis3set, axis4set, v_x, v_y, v_angle, " +
                        "moving, cpu0tem) " + "VALUES ("+ ts +")";
                //System.out.println(sql);
                DEBUG_DATA.put(i, sql);
                statement.addBatch(sql);
            }
            int[] res = statement.executeBatch();
            System.out.println("len of reslut:" + res.length);
            System.out.println("error sql");
            for(int i = 0; i < res.length; i++){
                if (res[i] == -1){
                    System.out.println(DEBUG_DATA.get(i));
                }
            }
//                if (hasResultSet) {
//                    ResultSet res = statement.getResultSet();
//                    while (res.next()) {
//                        System.out.println(res.getString("Time"));
//                    }
//                }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(statement != null){
                statement.close();
            }
            if(connection != null){
                connection.close();
            }
        }
    }
}
