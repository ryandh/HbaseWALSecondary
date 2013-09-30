package com.androidyou.extension;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.WALObserver;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.LoggerFactory;

public class SimpleWALObserver implements WALObserver {

	org.slf4j.Logger log = LoggerFactory.getLogger(SimpleWALObserver.class);
	java.util.logging.Level infolevel = java.util.logging.Level.INFO;
	SolrServer server;
	public void start(CoprocessorEnvironment env) throws IOException {
		server = new HttpSolrServer("http://localhost:8983/solr/collection1");

		myLog("Started");
	}

	private void myLog(String msg) {
		log.info("----" + msg + "---");
	}

	public void stop(CoprocessorEnvironment env) throws IOException {
		// TODO Auto-generated method stub
		server.shutdown();
		myLog("Stopped");
	}

	public boolean preWALWrite(ObserverContext<WALCoprocessorEnvironment> ctx,
			HRegionInfo info, HLogKey key, WALEdit edit) throws IOException {
		// get the document and send to solr

		String tableName = Bytes.toString(key.getTablename());
		myLog("TB--" + tableName);

		List<KeyValue> kvs = edit.getKeyValues();
		for (KeyValue kv : kvs) {
		//	kv.get
			
		 
			String rowkey=Bytes.toString(kv.getKey());
			
			String cf= Bytes.toString(kv.getFamily());
			String qualifer=Bytes.toString(kv.getQualifier());
			
			rowkey=rowkey.substring(0,rowkey.indexOf(cf+qualifer));
			
			String value=Bytes.toString(kv.getValue());
			
			//only maptch CF=c1, and qualifer=Address, 
			if(cf.equalsIgnoreCase("c1") && qualifer.equalsIgnoreCase("cat"))
			{
				myLog(String.format("posting to  solr %s:%s with key %s, and value %s",cf,qualifer,rowkey,value));
			
				SolrInputDocument doc = new SolrInputDocument();
				doc.addField("id", rowkey.trim());
				doc.addField("cat", value);
				try {
					server.add(doc);
					server.commit();
				} catch (SolrServerException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					myLog(e.getMessage());
				}
			 
			
				//Constrct a solr document and post it
			}
		}

		// Extract the Rowkey
		// CO:address
		// send to to solr

		return false;
	}

	public void postWALWrite(ObserverContext<WALCoprocessorEnvironment> ctx,
			HRegionInfo info, HLogKey logKey, WALEdit logEdit)
			throws IOException {
		// TODO Auto-generated method stub

	}

}
