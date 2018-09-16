import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Progressable;
/*
@author : sxk162731
 */
public class Part1 {
    public static void main(String[] args) throws Exception {
        /*
        store user hadoop url
         */
        String hadoopUrl = args[0];
        String baseUrl = "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/";

        ArrayList<String> subUrl = new ArrayList<String>();
        subUrl.add("20417.txt.bz2");
        subUrl.add("5000-8.txt.bz2");
        subUrl.add("132.txt.bz2");
        subUrl.add("1661-8.txt.bz2");
        subUrl.add("972.txt.bz2");
        subUrl.add("19699.txt.bz2");

        for(String itrUrl: subUrl) {
            String src = baseUrl+itrUrl;
            String dst = hadoopUrl+itrUrl;
            /*
            Create url and open urlConnection
             */
            URL url = new URL(src);
            URLConnection connection = url.openConnection();
            String redirect = connection.getHeaderField("Location");
            if (redirect != null){
                connection = new URL(redirect).openConnection();
            }
            InputStream inputStream = connection.getInputStream();
            /*
            Create configurations and add resources
             */
            Configuration conf = new Configuration();
            conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
            conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
            FileSystem fs = FileSystem.get(URI.create(dst), conf);
            OutputStream outputStream = fs.create(new Path(dst), new Progressable() {
                public void progress() {
                    System.out.print(".");
                }
            });

            IOUtils.copyBytes(inputStream, outputStream, 4096, true);

            /*
            Decompress .bz2 file
             */
            Path inputPath = new Path(dst);
            CompressionCodecFactory factory = new CompressionCodecFactory(conf);
            CompressionCodec codec = factory.getCodec(inputPath);
            if (codec == null) {
                System.err.println("No codec found for " + dst);
                System.exit(1);
            }

            String outputUri =
                    CompressionCodecFactory.removeSuffix(dst, codec.getDefaultExtension());

            inputStream = null;
            OutputStream outDecompress = null;
            try {
                inputStream = codec.createInputStream(fs.open(inputPath));
                outDecompress = fs.create(new Path(outputUri));

                IOUtils.copyBytes(inputStream, outDecompress, conf);

                if(fs.exists(inputPath)) {
                    fs.delete(inputPath, true);
                } else {
                    System.out.println("[Error]: File is not present inputStream your HDFS!!");
                }

            } finally {
                /*
                close all Streams
                 */
                IOUtils.closeStream(inputStream);
                IOUtils.closeStream(outDecompress);
                IOUtils.closeStream(outputStream);
            }
        }
    }
}