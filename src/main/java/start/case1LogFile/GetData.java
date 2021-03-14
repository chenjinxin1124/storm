package start.case1LogFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

public class GetData {
    public static void main(String[] args) {
        String[] hosts = {"bigdata-pro00", "bigdata-pro01", "bigdata-pro02", "bigdata-pro03", "bigdata-pro04"};
        String[] time = {"2020-01-01 12:12", "2020-02-01 12:12", "2020-03-01 12:12", "2020-04-01 12:12", "2020-05-01 12:12",};
        String[] serviceName = {"Hadoop", "Kafka", "Spark", "Storm", "Flink"};

        Random random = new Random();
        File logFile = new File("/home/opt/datas/track.log");
        StringBuffer str = new StringBuffer();
        FileOutputStream fos = null;
        try {
            for (int i = 0; i < 100; i++) {
                str.append(hosts[random.nextInt(5)]).append("\t");
                str.append(time[random.nextInt(5)]).append("\t");
                str.append(serviceName[random.nextInt(5)]).append("\n");
            }
            if (!logFile.exists()) {
                logFile.createNewFile();
            }
            fos = new FileOutputStream(logFile);
            fos.write(str.toString().getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            str = null;
        }


    }
}
