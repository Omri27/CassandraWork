

import java.io.*;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
//Omri Kodzidlo - phone number - 0506790773
public class Assignment {
    public static void main(String[] args) {
        int chunkSize = 10000;
        String urlString = "https://www.ynet.co.il/";
        String inputFileName = "Download.html";
        try {
            CassandraApi cassandra = new CassandraApi();

            cassandra.init(); // initiate cassandra API

            cassandra.createKeySpace(); // creating the keyspace

            cassandra.createTable(); // creating the table

            DownloadWebPage(urlString,inputFileName); // downloading the webpage to a file

            cassandra.InsertData(inputFileName, chunkSize, urlString); // inserting the chunks of data from the file to cassandra

            cassandra.FetchData(urlString); // fetching the data from cassandra table

            System.out.println("Assignment has finished :)");

            System.exit(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void DownloadWebPage(String webpage, String inputFileName)
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


}
