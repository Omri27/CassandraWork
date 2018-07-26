

import com.datastax.driver.core.*;

import java.io.*;

public class CassandraApi {
    private static Session session = null;
    private static Cluster cluster = null;
    private PreparedStatement stmt = null;
    public void init() {
        cluster = Cluster.builder() // connecting to the cluster
                .addContactPoint("127.0.0.1")
                .build();
        session = cluster.connect();
    }

    public void createTable() {
        String createTableQueryString = "CREATE TABLE IF NOT EXISTS VoyagerLabs.contents (" + // creating a table if not exists in keyspace.
                "  part_number int," +
                "  url varchar," +
                "  slice text," +
                "  PRIMARY KEY (url, part_number));"; // partitioned by Url
        session.execute(createTableQueryString);
    }

    public void createKeySpace() { //creating keyspace if not exist.
        String createKeySpaceQueryString = "CREATE KEYSPACE IF NOT EXISTS VoyagerLabs WITH replication " +
                "                    = {'class':'SimpleStrategy', 'replication_factor':3};"; // replicate by 3 nodes
        session.execute(createKeySpaceQueryString);
    }

    public void InsertData(String readFromFile, int chunkSize, String urlString) throws IOException {
        String insertQuery = "INSERT INTO VoyagerLabs.contents (part_number, url, slice) "
                + "VALUES (?,?,?) ";
        stmt  = session.prepare(insertQuery); // preparing statement for loading data
        RandomAccessFile fileReader = new RandomAccessFile(readFromFile,"r");
        long sourceSize = fileReader.length(); // getting source file size
        long numOfReads = sourceSize/chunkSize; // calculating how many reads according to chunk size
        long remainingBytes = sourceSize % chunkSize; // calculating the remainder
        int split = 0;
        for(int i =0; i < numOfReads; i++){ // loading the data
            split++;
            loadData(fileReader, chunkSize,i, urlString);
        }
        if(remainingBytes > 0){ // loading the remainder of the data
            System.out.println("last batch of byte");
            loadData(fileReader, chunkSize, split++, urlString);
        }
        fileReader.close();
    }

    private void loadData(RandomAccessFile fileReader, long numOfBytes, int partNumber, String urlString) throws IOException{
        byte[] buffer = new byte[(int)numOfBytes];
        int val = fileReader.read(buffer);
        if(val != -1){ // as long as there is data to insert
            String converted  = new String(buffer,"UTF-8"); // converting data to UTF-8 TYPE
            try{
                System.out.println("==== inserted Chunk==" + partNumber);
                BoundStatement bound = new BoundStatement(stmt);
                session.execute(bound.bind(partNumber,urlString,converted)); // inserting data to the table
            }catch (Exception e){
                e.printStackTrace();
                System.out.println("=====>>>>" + e.getMessage());
                System.exit(0);
            }
        }
    }
    public void FetchData(String urlString) throws IOException{
        String scql = "SELECT part_number, url from VoyagerLabs.contents where url = '"+ urlString +"'";
        ResultSet rs = session.execute(scql);
        for(Row row : rs){
            if(rs.getAvailableWithoutFetching() == 5 && !rs.isFullyFetched()){ // fetching the data out of the table
                rs.fetchMoreResults();
            }
            int partNumber = row.getInt("part_number");
            String url = row.getString("url");
            System.out.println("Selected Chunk==" + partNumber +"; url == " + url);
        }
    }
}
