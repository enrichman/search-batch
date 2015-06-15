import it.enricocandino.extractor.model.Response;
import it.enricocandino.extractor.reader.ClueWebReader;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Enrico Candino
 */
public class Extractor {

    private static final String BASE_FOLDER = "/Users/enrico/Documents/warc/";

    public static void main(String[] args) {
        System.out.println("start: "+new Date());



        long startBatch = System.currentTimeMillis();
        long start = System.currentTimeMillis();

        try {

            for (int i = 9; i < 10; i++) {

                String path = BASE_FOLDER + "0" + i + ".warc.gz";

                System.out.println("Start reading from: "+path);
                //List<Response> responses = reader.read(path);

                Queue<Response> queue = new ConcurrentLinkedQueue<Response>();
                ExecutorService producer = Executors.newFixedThreadPool(12);
                ExecutorService consumer = Executors.newFixedThreadPool(8);

                ClueWebReader reader = new ClueWebReader(producer, consumer, queue);
                reader.read(path);

                producer.shutdown();
                consumer.shutdown();

                while (!producer.isTerminated()) {}
                System.out.println("Producers["+i+"] ended!");

                while (!consumer.isTerminated()) {}
                System.out.println("Consumers["+i+"] ended!");

                /*
                for (Response response : responses) {
                    Thread worker = new SolrThread(response);
                    executor.execute(worker);
                }
                */
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        long startCommit = System.currentTimeMillis();
        //SolrIndexer.commit();

        System.out.println("Finish commit in " + (System.currentTimeMillis() - startCommit));

        System.out.println("Finish indexing in " + (System.currentTimeMillis() - start));
        System.out.println("Finish batch in " + (System.currentTimeMillis() - startBatch));

        System.out.println("** Completed! **");

        System.out.println("end: "+new Date());
    }

    // chops a list into non-view sublists of length L
    static <T> List<List<T>> chopped(List<T> list, final int L) {
        List<List<T>> parts = new ArrayList<List<T>>();
        final int N = list.size();
        for (int i = 0; i < N; i += L) {
            parts.add(new ArrayList<T>(
                            list.subList(i, Math.min(N, i + L)))
            );
        }
        return parts;
    }

}
