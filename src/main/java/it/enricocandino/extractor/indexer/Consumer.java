package it.enricocandino.extractor.indexer;


import it.enricocandino.extractor.model.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;

public class Consumer implements Runnable {

    private enum Log {
        me;
        private int count;

        public void done(int docs) {
            count += docs;
            if(count % 100 == 0)
                System.out.println("Index Done ["+count+"]");
        }
    }

    private List<Response> responses;

    private Queue<Response> queue;
    private ExecutorService producer;

    public Consumer(Queue<Response> queue, ExecutorService producer) {
        this.responses = new ArrayList<Response>();
        this.queue = queue;
        this.producer = producer;
    }

    public void run() {
        while (!producer.isTerminated()) {
            Response r = queue.poll();
            if (r != null) {
                responses.add(r);
            }
            if (responses.size() == 1000) {
                System.out.println("INDEXING from " + Thread.currentThread().getId());

                Solr.Client.index(responses);
                Log.me.done(1000);

                responses.clear();
            }
        }

        while (!queue.isEmpty()) {
            Response r = queue.poll();
            if (r != null) {
                responses.add(r);
            }
        }

        if(!responses.isEmpty()) {
            System.out.println("INDEXING FINAL from " + Thread.currentThread().getId());
            Solr.Client.index(responses);

            Log.me.done(responses.size());
        }
    }

}
