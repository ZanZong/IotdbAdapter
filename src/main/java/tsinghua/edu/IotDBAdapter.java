package tsinghua.edu;

import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;



public class IotDBAdapter {

    private static String LOCALTMPDIR = "/tmp/flok_iotdb/";
    private static String LOCALTMPFILE = LOCALTMPDIR + "iotdb_trans.csv";

    private void mkdir(String path) {
        String Path = LOCALTMPDIR;
        File file = new File(Path);
        if(!file.exists()){
            file.mkdirs();
        }
    }

    private void copyFromLocal(String hdfspath, String hdfsHost) {
        Configuration conf=new Configuration();
        conf.set("fs.default.name", hdfsHost);
        try {
            FileSystem hdfs=FileSystem.get(conf);
            // local file
            Path src =new Path(IotDBAdapter.LOCALTMPFILE);
            // path in hdfs
            Path dst =new Path(hdfspath);
            hdfs.copyFromLocalFile(src, dst);
            System.out.println("Upload to " + dst);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){

        SparkConf sparkConf = new SparkConf().setAppName("IotDBLoader");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        if (args.length < 5) {
            System.out.println("Usage:\n");
            System.out.println("\t [preix path of timeseries] [path of output data in HDFS] [database url] [username] [password]");
            System.out.println("\te.g. root.yanmoji.shenzhen.d1 " +
                    "hdfs:<ip>:<port>/data/iotdb_out.csv localhost:6667/ root root");
            return;
        }
        String intactPath = args[0];
        String prefixPath = args[1];
        String dburl = args[2];
        String username = args[3];
        String password = args[4];
        String[] tmpStrings = intactPath.split("/");
        String hdfsHost = tmpStrings[0] + "//" + tmpStrings[2];
        StringBuilder sb = new StringBuilder();
        sb.append("/");
        for(int i = 3; i < tmpStrings.length; i++) {
            sb.append(tmpStrings[i]);
            sb.append("/");
        }
        String hdfsPath = sb.toString().substring(0, sb.length() - 1);
        System.out.println("HDFS Host:" + hdfsHost);
        System.out.println("HDFS Path:" + hdfsPath);
        System.out.println("Connect to database:" + dburl);

        IotDBAdapter iotDBAdapter = new IotDBAdapter();
        iotDBAdapter.mkdir(IotDBAdapter.LOCALTMPDIR);

        BufferedWriter bufferedWriter = null;
        File outFile = null;
        try {
            LoadData dataloader = new LoadData();
            outFile = new File(IotDBAdapter.LOCALTMPFILE);
            try {
                bufferedWriter = new BufferedWriter(new FileWriter(outFile));
            } catch (IOException e) {
                e.printStackTrace();
            }
            dataloader.dataTransfer(prefixPath, bufferedWriter, "jdbc:tsfile://" + dburl, username, password);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                bufferedWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        iotDBAdapter.copyFromLocal(hdfsPath, hdfsHost);

        ctx.stop();
    }
}
