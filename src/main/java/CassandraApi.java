import com.datastax.driver.core.*;

import java.io.*;

public class CassandraApi {
    private static Session session = null;
    private static Cluster cluster = null;
    private static String keySpace = "";
    private PreparedStatement stmt = null;
    public void init() {
        cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .build();
        session = cluster.connect();
    }

    public void createTable() {
        String createTableQueryString = "CREATE TABLE IF NOT EXISTS simplex.contents (" +
                "  part_number int," +
                "  url varchar," +
                "  slice text," +
                "  PRIMARY KEY (url, part_number));";
        session.execute(createTableQueryString);
    }

    public void createKeySpace() {
        String createKeySpaceQueryString = "CREATE KEYSPACE IF NOT EXISTS simplex WITH replication " +
                "                    = {'class':'SimpleStrategy', 'replication_factor':3};";
        session.execute(createKeySpaceQueryString);
    }

    public void InsertData(String ipFile, int chunkSize, String urlString) throws IOException {
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
            loadData(raf, maxReadBufferSize,i, urlString);
        }
        if(remainingBytes > 0){
            System.out.println("last batch of byte");
            loadData(raf, maxReadBufferSize, split++, urlString);
        }
        raf.close();
    }
    public void loadData(RandomAccessFile raf, long numBytes, int splitno, String urlString) throws IOException{
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
    public void FetchData(String opFile, String urlString) throws IOException{
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
