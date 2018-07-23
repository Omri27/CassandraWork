import com.datastax.driver.core.*;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Random;
import java.util.UUID;

public class Assignment {
    public static Session session = null;
    public static Cluster cluster = null;
    public static PreparedStatement stmt = null;
    public static String urlString = "https://www.ynet.co.il/";
    public static String inputFileName = "Download.html";
    public static String outFileName = "outDownload.html";
    public static int chunkSize = 10000;

    public static void main(String[] args) {
        try {
            cluster = Cluster.builder()
                    .addContactPoint("127.0.0.1")
                    .build();
            session = cluster.connect();
            String dropQueryString = "DROP KEYSPACE simplex;";
            session.execute(dropQueryString);
            /////////////////////////////////////////
            String createKeySpaceQueryString = "CREATE KEYSPACE simplex WITH replication " +
                    "                    = {'class':'SimpleStrategy', 'replication_factor':3};";
            session.execute(createKeySpaceQueryString);

            ////////////////////////
            String createTableQueryString = "CREATE TABLE simplex.contents (" +
                    "  part_number int," +
                    "  url varchar," +
                    "  slice text," +
                    "  PRIMARY KEY (url, part_number));";
            session.execute(createTableQueryString);
           ///////////////////////////////////
            DownloadWebPage(urlString);

                insertData(inputFileName, chunkSize);
            fetchData(outFileName);
            System.out.println("Cassandra connection established");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (cluster != null) cluster.close();
        }
    }

    public static void DownloadWebPage(String webpage)
    {
        try {

            // Create URL object
            URL url = new URL(webpage);
            BufferedReader readr =
                    new BufferedReader(new InputStreamReader(url.openStream()));

            // Enter filename in which you want to download
            BufferedWriter writer =
                    new BufferedWriter(new FileWriter(inputFileName));

            // read each line from stream till end
            String line;
            while ((line = readr.readLine()) != null) {
                writer.write(line);
            }

            readr.close();
            writer.close();
            System.out.println("Successfully Downloaded.");
        }

        // Exceptions
        catch (MalformedURLException mue) {
            System.out.println("Malformed URL Exception raised");
        }
        catch (IOException ie) {
            System.out.println("IOException raised");
        }
    }
    public static void insertData(String ipFile, int chunkSize) throws IOException{
        String insertQuery = "INSERT INTO simplex.contents (part_number, url, slice) "
                + "VALUES (?,?,?) ";
         stmt  = session.prepare(insertQuery);
        File fName = new File(ipFile);
        RandomAccessFile raf = new RandomAccessFile(ipFile,"r");
        long sourceSize = raf.length();
        int maxReadBufferSize = chunkSize;
        long numReads = sourceSize/maxReadBufferSize;
        long remainingBytes = sourceSize % maxReadBufferSize;
        int split = 0;
        for(int i =0; i < numReads; i++){
            split++;
            loadData(raf, maxReadBufferSize,i);
        }
        if(remainingBytes > 0){
            System.out.println("last batch of byte");
            loadData(raf, maxReadBufferSize, split++);
        }
        raf.close();
    }
    public static void loadData(RandomAccessFile raf, long numBytes, int splitno) throws IOException{
        byte[] buf = new byte[(int)numBytes];
        int val = raf.read(buf);
        if(val != -1){
            String converted  = new String(buf,"UTF-8");
            try{
                System.out.println("==== inserted Chunk==" + splitno);
                BoundStatement bound = new BoundStatement(stmt);
                session.execute(bound.bind(splitno,urlString,converted));
            }catch (Exception e){
                e.printStackTrace();
                System.out.println("=====>>>>" + e.getMessage());
                System.exit(0);
            }
        }
    }
    public static void fetchData(String opFile) throws IOException{
        String scql = "SELECT part_number, url, slice from simplex.contents where url = '"+ urlString +"'";
        OutputStreamWriter fw = new OutputStreamWriter(new FileOutputStream(opFile), "UTF-8");
        SimpleStatement ss = (SimpleStatement) new SimpleStatement(scql)/*.setReadTimeoutMillis(65000)*/;
        ss.setFetchSize(5);
        ResultSet rs = session.execute(ss);
        for(Row row : rs){
            if(rs.getAvailableWithoutFetching() == 5 && !rs.isFullyFetched()){
                rs.fetchMoreResults();
            }
            int splitno = row.getInt("part_number");
            String url = row.getString("url");
            String slice = row.getString("slice");
            fw.write(slice);
            System.out.println("Selected Chunk==" + splitno +"; url == " + url);
        }
        fw.close();
    }
}
