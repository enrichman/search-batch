package it.enricocandino.extractor.indexer;


import it.enricocandino.extractor.model.Response;

public class SolrThread extends Thread {
	
	private Response response;
	
	public SolrThread(Response response) {
		this.response = response;
	}
	
	@Override
	public void run() {
		SolrIndexer.index(response);
	}

}
