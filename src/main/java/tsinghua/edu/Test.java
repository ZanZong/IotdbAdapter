package tsinghua.edu;

public class Test {
    public static void main(String[] args) {
        String s = "hdfs://192.168.10.10:9000/data/test/6/IotDBLoader_f499dbdb-7242-405a-925d-a08c9b431028_0.output";
        for(String s1 : s.split("/")) {
            System.out.println(s1);
        }
    }
}
