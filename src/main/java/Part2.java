import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
/*
@author : sxk162731
 */
public class Part2 {

    public static void main( String[] args ) throws IOException
    {
        /*
         src : location from where zip file needs to be read
         dst : hdfs location where you need to dump the decompressed zip file
         */
        String src = args[0];
        String dst = args[1];

        URL url = new URL(src);
        /*
        open the connection from src url location
         */
        URLConnection connection = url.openConnection();
        String redirect = connection.getHeaderField("Location");
        if(redirect != null) {
            connection = new URL(redirect).openConnection();
        }

        InputStream inputStream = connection.getInputStream();

        ZipInputStream zipInputStream;
        zipInputStream = new ZipInputStream(inputStream);

        /*
        Create your configuration file and  required resources
         */
        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));

        ZipEntry entry = zipInputStream.getNextEntry();

        /*
        Iterate over each entry of zip file
         */
        while (entry != null) {
            FileSystem fs = FileSystem.get(URI.create(dst), conf);
            if (!entry.isDirectory()) {
                OutputStream out;
                /*
                check if entry is a txt file
                 */
                if(entry.getName().contains(".txt")){
                    out = fs.create(new Path(args[1]+entry.getName()), new Progressable() {
                        public void progress() {
                            System.out.print(".");
                        }
                    });
                    IOUtils.copyBytes(zipInputStream, out, 4096, false);
                }

            }
            /*
            fetch the next entry
             */
            entry = zipInputStream.getNextEntry();
        }

        /*
        Close the opened streams
         */
        IOUtils.closeStream(zipInputStream);
        IOUtils.closeStream(inputStream);

        System.out.println("SUCCESS");
    }
}
