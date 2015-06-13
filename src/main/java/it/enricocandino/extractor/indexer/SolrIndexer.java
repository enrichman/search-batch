package it.enricocandino.extractor.indexer;

import java.io.IOException;

import it.enricocandino.extractor.model.Response;
import it.enricocandino.extractor.util.SafeCheck;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

public class SolrIndexer {

	private static String url = "http://localhost:8983/solr/warc_core";
	private static SolrClient solrClient = new HttpSolrClient(url);

	public static void index(Response response) {
		try {
			solrClient.add(createResponseDocument(response));
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void commit() throws SolrServerException, IOException {
		solrClient.commit();
	}
	
	private static SolrInputDocument createResponseDocument(Response response) {
		SolrInputDocument doc = new SolrInputDocument();
		doc.addField("id", response.getUrl());
		doc.addField("title", response.getTitle());
		doc.addField("text", response.getText());

		boolean isSafe = SafeCheck.INSTANCE.isSafe(response.getText());
		doc.addField("safe", isSafe);

		return doc;
	}

}
