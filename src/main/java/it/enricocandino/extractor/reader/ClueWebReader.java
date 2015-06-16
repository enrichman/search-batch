package it.enricocandino.extractor.reader;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.zip.GZIPInputStream;

import clueweb09.WarcRecord;
import it.enricocandino.extractor.indexer.Producer;
import it.enricocandino.extractor.indexer.Consumer;
import it.enricocandino.extractor.model.Response;


public class ClueWebReader {

    private Queue<Response> queue;
    private ExecutorService producer;
    private ExecutorService consumer;

    public ClueWebReader(ExecutorService producer, ExecutorService consumer, Queue<Response> queue) {
        this.producer = producer;
        this.consumer = consumer;
        this.queue = queue;
    }

    public List<Response> read(String path) throws Exception {
        long start = System.currentTimeMillis();

        GZIPInputStream gzInputStream = new GZIPInputStream(new FileInputStream(path));
        DataInputStream inStream = new DataInputStream(new BufferedInputStream(gzInputStream));

        List<Response> responses = new ArrayList<Response>();

        WarcRecord thisWarcRecord;
        int count = 1;

        while ((thisWarcRecord = WarcRecord.readNextWarcRecord(inStream)) != null) {

            if(count%100 == 0)
                System.out.println("["+count+"]");
            count++;

            producer.execute(new Producer(queue, thisWarcRecord));
            consumer.execute(new Consumer(queue, producer));
        }

        inStream.close();

        System.out.println("Finish read in "+(System.currentTimeMillis()-start));

        return responses;
    }

}
