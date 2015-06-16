package it.enricocandino.extractor.indexer;

import clueweb09.WarcHTMLResponseRecord;
import clueweb09.WarcRecord;
import it.enricocandino.extractor.model.Response;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.util.Queue;
import java.util.Scanner;

/**
 * @author Enrico Candino
 */
public class Producer implements Runnable {

    private enum Log {
        me;
        private int count;

        public void done() {
            count++;
            if(count % 100 == 0)
                System.out.println("Done ["+count+"]");
        }
    }

    private WarcRecord warcRecord;
    private Queue<Response> queue;

    public Producer(Queue<Response> queue, WarcRecord warcRecord) {
        this.queue = queue;
        this.warcRecord = warcRecord;
    }

    public void run() {
        try {
            if (warcRecord.getHeaderRecordType().equals("response")) {
                WarcHTMLResponseRecord htmlRecord = new WarcHTMLResponseRecord(warcRecord);
                String url = htmlRecord.getTargetURI();

                String rawResponse = new String(htmlRecord.getRawRecord().getByteContent(), "iso-8859-1");

                boolean isTextFile = false;
                boolean headerRead = false;

                StringBuilder responseBuilder = new StringBuilder(htmlRecord.getRawRecord().getTotalRecordLength());

                Scanner scanner = new Scanner(rawResponse);
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    if (line.startsWith("Content-Type:") && line.contains("text")) {
                        isTextFile = true;
                    }

                    // header end: check if is text file or is to skip
                    if (line.equals("") || headerRead) {
                        headerRead = true;
                        if (!isTextFile) {
                            break;
                        } else {
                            responseBuilder.append(line);
                        }
                    }
                }
                scanner.close();

                String responseBody = responseBuilder.toString();

                responseBody = responseBody.trim();
                if (responseBody.length() > 0) {

                    Document doc = Jsoup.parse(responseBody);
                    Elements titleElem = doc.select("title");
                    String title = "";
                    if (titleElem != null && titleElem.size() > 0)
                        title = titleElem.get(0).text();

                    String bodyText = "";
                    if (doc.body() != null)
                        bodyText = doc.body().text();

                    Response response = new Response();
                    response.setUrl(url);
                    response.setTitle(title);
                    response.setText(bodyText);
                    queue.add(response);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        Log.me.done();
    }
}
