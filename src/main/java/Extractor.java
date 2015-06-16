import it.enricocandino.extractor.indexer.Solr;
import it.enricocandino.extractor.model.Response;
import it.enricocandino.extractor.reader.ClueWebReader;

import java.io.FileInputStream;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Enrico Candino
 */
public class Extractor {

    public static void main(String[] args) {

        if(args.length == 0) {
            System.out.print("ERROR: missing arguments with the warc paths");
            return;
        }

        try {
            Properties props = new Properties();
            props.load(new FileInputStream("config.properties"));
            String solrHost = props.getProperty("solr.host");
            Solr.Client.init(solrHost);

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error loading properties: exit!");
            return;
        }

        System.out.println("start: "+new Date());

        long startBatch = System.currentTimeMillis();
        long start = System.currentTimeMillis();

        Queue<Response> queue = new ConcurrentLinkedQueue<Response>();
        ExecutorService producer = Executors.newFixedThreadPool(30);
        ExecutorService consumer = Executors.newFixedThreadPool(20);
        ClueWebReader reader = new ClueWebReader(producer, consumer, queue);

        try {
            for (String path : args) {
                System.out.println("Start reading from: "+path);

                reader.read(path);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        producer.shutdown();
        consumer.shutdown();

        while (!producer.isTerminated()) {}
        System.out.println("Producers ended!");

        while (!consumer.isTerminated()) {}
        System.out.println("Consumers ended!");


        long startCommit = System.currentTimeMillis();
        //SolrIndexer.commit();
        System.out.println("Finish commit in " + (System.currentTimeMillis() - startCommit));

        System.out.println("Finish indexing in " + (System.currentTimeMillis() - start));
        System.out.println("Finish batch in " + (System.currentTimeMillis() - startBatch));
        System.out.println("** Completed! **");
        System.out.println("end: "+new Date());
    }

}
