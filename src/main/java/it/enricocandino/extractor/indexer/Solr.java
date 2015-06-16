package it.enricocandino.extractor.indexer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import it.enricocandino.extractor.model.Response;
import it.enricocandino.extractor.util.SafeCheck;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

public enum Solr {

	Client;

	private SolrClient solrClient;

	public void init(String solrPath) {
		if(this.solrClient == null)
			this.solrClient = new HttpSolrClient(solrPath);
	}

	public void index(List<Response> responses) {
		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		for(Response r : responses) {
			docs.add(createResponseDocument(r));
		}
		try {
			solrClient.add(docs, 10000);
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void commit() {
		try {
			solrClient.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private SolrInputDocument createResponseDocument(Response response) {
		SolrInputDocument doc = new SolrInputDocument();
		doc.addField("id", response.getUrl());
		doc.addField("title", response.getTitle());
		doc.addField("text", response.getText());

		boolean isSafe = SafeCheck.INSTANCE.isSafe(response.getText());
		doc.addField("safe", isSafe);

		return doc;
	}

}
