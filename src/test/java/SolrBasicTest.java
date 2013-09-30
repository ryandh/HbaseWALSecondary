

import java.io.IOException;
import java.util.Collection;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.eclipse.jdt.internal.core.Assert;
import org.junit.Before;
import org.junit.Test;

public class SolrBasicTest {

	@Test
	public void testInsert() throws SolrServerException, IOException {
		SolrInputDocument doc = new SolrInputDocument();
		doc.addField("id", "1");
		doc.addField("name", "Hello Solr");
		server.add(doc);
	 
		server.commit();
		// junit.framework.Assert.assertEquals(200, resp.getStatus());
	}

	@Test
	public void TestGetMatch() throws SolrServerException, IOException {
		ModifiableSolrParams params = new ModifiableSolrParams();
		params.set("q", "id:1");
		QueryResponse res = server.query(params);
		SolrDocumentList result = res.getResults();
		Assert.isNotNull(result);
		junit.framework.Assert.assertEquals(1, result.getNumFound());
	}

	SolrServer server = new HttpSolrServer("http://localhost:9999/solr/collection1");

	@Before
	public void SetupSolrConnection() {
		// org.apache.http.pool
		

	}

}
