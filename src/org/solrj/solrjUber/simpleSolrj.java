package org.solrj.solrjUber;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;


import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.json.JSONException;


public class simpleSolrj {

	public static void main(String[] args) throws SolrServerException, IOException, JSONException {
		// TODO Auto-generated method stub
		BufferedWriter bw=null;
		String createdDtmfrom= args[0];
		String createdDtmto = args[1];
		String fileName = args[2];
		FileWriter writer = new FileWriter(fileName);
		//FileWriter writer = new FileWriter("C:\\Users\\311141.CTS\\Documents\\workspace-sts-3.9.0.RELEASE\\solrjUber\\shortOutput");  
		String url = "http://10.65.133.126:8987/solr/glb_partner_api_store.feed_item_status_index";
		//String urlString = "http://solr.cdc.uber-item-prod.ms-df-solrcloud.glb2.prod.walmart.com:8983/solr/flat_product_index";
		SolrClient solr = new HttpSolrClient.Builder(url).build();
		//((HttpSolrClient) solr).setParser(new XMLResponseParser());
		
		SolrQuery query = new SolrQuery();
		query.setFields("feed_id","created_dtm","item_processing_type","item_processing_state","item_processing_status","seller_id","error");
		query.set("q", "created_dtm:[\""+createdDtmfrom+"\" TO \""+createdDtmto+"\"] AND -item_processing_status:(SUCCESS TIMEOUT_ERROR) AND item_processing_type:(MP_ITEM_CREATE MP_ITEM_REF_ONLY MP_ITEM_RETIRE MP_ITEM_UPDATE MP_OFFER_ENVELOPE MP_PRODUCT_ENVELOPE)");
		//SolrQuery q=new SolrQuery();
		System.out.println("solr is running");
		query.set("indent","true");
		QueryResponse response = solr.query(query);
		SolrDocumentList docList = response.getResults();
		System.out.println(docList);
		long numFound = docList.getNumFound();
		System.out.println("Actual numFound : "+numFound);
		System.out.println("before start : "+System.currentTimeMillis());
		int current = 1;
		int maxRows = 1000;
		
//		numFound=numFound/2;
		System.out.println("current numFound :"+numFound);
		bw = new BufferedWriter(writer);
	    while (numFound > 0) {
//	    	System.out.println("current value is :"+current);
	    	
	    	query.setStart(current);
			query.setRows(maxRows);
			QueryResponse responseData = solr.query(query);
			SolrDocumentList docListIter = responseData.getResults();
			//System.out.println(docListIter);
//			System.out.println("list done : "+current+ " and "+numFound);
			String docs = docListIter.toString();
			bw.write(docs);
			current=current+maxRows;
			numFound = numFound-maxRows;
		
			/*JSONObject returnResults = new JSONObject();
			Map<Integer, Object> solrDocMap = new HashMap<Integer, Object>();
			int counter = 1;
			for(Map singleDoc : docListIter)
			{
			  solrDocMap.put(counter, new JSONObject(singleDoc));
			  counter++;
			}
			returnResults.put("docs", solrDocMap);
			
			current=current+10000;
			System.out.println(returnResults);*/
			
	    }
	    System.out.println("after Complete"+System.currentTimeMillis());
		//System.out.println("print all :"+docList);

		bw.close();
		
		
	}

}
