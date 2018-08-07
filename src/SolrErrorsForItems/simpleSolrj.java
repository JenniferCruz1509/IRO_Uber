package SolrErrorsForItems;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.noggit.JSONParser;


public class simpleSolrj {

	public static void main(String[] args) throws SolrServerException, IOException, JSONException {
		// TODO Auto-generated method stub
		BufferedWriter bw=null;
		String createdDtmfrom= args[0];
		String createdDtmto = args[1];
		String fileName = args[2];
		//FileWriter writer = new FileWriter(fileName);
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
		  SolrDocumentList results = response.getResults();
	        for (int i = 0; i < results.size(); ++i) {
	        	JSONObject returnResults = new JSONObject(results.get(i));
	            //System.out.println("from json "+returnResults);
	                     
	           JSONArray arrayError =  (JSONArray) returnResults.get("error");
	           System.out.println("ArrayData "+arrayError);
	          for(int j=0;j<arrayError.length();j++)
	          {
	        	 String code =  (String) arrayError.get(j);
	        	 JSONObject jsonObj = new JSONObject(code);
	        	 String errorCode = jsonObj.getString("code");
	        	 //String errorCauses = jsonObj.getString("causes");
	        	 String errorDescription = jsonObj.getString("description");
	        	 String errorInfo = jsonObj.getString("info");

	        	 
	        	  System.out.println("code : "+errorCode);
	        	  //System.out.println("Cause : "+errorCauses);
	        	  System.out.println("Description : "+errorDescription);
	        	  System.out.println("code : "+errorInfo);
	        	  
	        	 
	        	 
		        	         }
	        }
	        System.out.println("***********");
	      System.out.println(response.getFacetField("feed_id"));
	      NamedList<Object> resultsNamed = response.getResponse();
		  NamedList<Object> report = (NamedList<Object>)resultsNamed.get("feed_id");
		  System.out.println(report);
		
		
		SolrDocumentList docList = response.getResults();
		System.out.println(docList);
		long numFound = docList.getNumFound();
		SolrDocument data = docList.get(2);
		System.out.println(data);
		
		System.out.println("Actual numFound : "+numFound);
		
			
			
			/*Map<Integer, Object> solrDocMap = new HashMap<Integer, Object>();
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
	    //System.out.println("after Complete"+System.currentTimeMillis());
		//System.out.println("print all :"+docList);

		
		
	

}
