package org.elasticsearch;


import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class DocumentFetchTest {
    private  RestHighLevelClient client = new RestHighLevelClient( RestClient.builder(
            new HttpHost("localhost", 9200, "http")));;
    private final String sourceIndex = "index_test";

    @Before
    public void setUpBeforeClass() {
        this.client = new RestHighLevelClient( RestClient.builder(
                new HttpHost("localhost", 9200, "http")));
    }

    @After
    public void tearDownAfterClass() throws IOException {
        this.client.close();

    }

    @Test
    public void testIndex() throws IOException{
        {
            // prepare
            DocumentFetch.docModel doc = new DocumentFetch.docModel(1,"Green","A");
            IndexResponse indexResponse = DocumentFetch.InsertDocument(sourceIndex, "1", doc, client);
            assertEquals(RestStatus.OK, indexResponse.status());
        }
        {
            // insert document
            DocumentFetch.docModel doc = new DocumentFetch.docModel(10,"Green","A");
            IndexResponse indexResponse = DocumentFetch.InsertDocument(sourceIndex, "1", doc, client, true);
            assertEquals(RestStatus.OK, indexResponse.status());
            assertEquals(doc.toString(), indexResponse.getGetResult().sourceAsString());
        }
    }

    @Test
    public void testUpdate() throws IOException{
        DocumentFetch.docModel doc = new DocumentFetch.docModel(1,"Green","A");
        Script script = new Script("ctx._source.counter+=5");
        {
            // prepare
            IndexResponse indexResponse = DocumentFetch.InsertDocument(sourceIndex, "1", doc, client);
            assertEquals(RestStatus.OK, indexResponse.status());
        }
        {
            // update document and fetch old version
            UpdateResponse updateResponse = DocumentFetch.updateDocument(sourceIndex, "1", client, script, false, true);
            assertEquals(RestStatus.OK, updateResponse.status());
            assertEquals(doc.toString(), updateResponse.getGetResultOld().sourceAsString());
            doc.setCounter(doc.counter+5);
        }
        {
            // update document and fetch new version
            UpdateResponse updateResponse = DocumentFetch.updateDocument(sourceIndex, "1", client, script, true, false);
            assertEquals(RestStatus.OK, updateResponse.status());
            doc.setCounter(doc.counter+5);
            assertEquals(doc.toString(), updateResponse.getGetResult().sourceAsString());
        }
    }

    @Test
    public void testUpdateByQuery() throws IOException{
        DocumentFetch.docModel doc1 = new DocumentFetch.docModel(2,"Red","B");
        DocumentFetch.docModel doc2 = new DocumentFetch.docModel(2,"Brown","C");
        {
            // prepare
            assertEquals(RestStatus.OK, DocumentFetch.InsertDocument(sourceIndex,"2",doc1,client).status());
            assertEquals(RestStatus.OK, DocumentFetch.InsertDocument(sourceIndex,"3",doc2,client).status());
        }
        {
            // update by query and fetch old version of documents
            BulkByScrollResponse bulkByScrollResponse = DocumentFetch.updateByQueryDocument(sourceIndex, client,
                    new Script("ctx._source.counter+=5"),
                    new TermQueryBuilder("counter",2),
                    false,true);

            assertNotEquals(null, bulkByScrollResponse.getGetResultsOld());
            if (bulkByScrollResponse.getGetResultsOld().get(0).getId().equals("2")) {
                assertEquals(doc1.toString(), bulkByScrollResponse.getGetResultsOld().get(0).sourceAsString());
                assertEquals(doc2.toString(), bulkByScrollResponse.getGetResultsOld().get(1).sourceAsString());
            } else {
                assertEquals(doc2.toString(), bulkByScrollResponse.getGetResultsOld().get(0).sourceAsString());
                assertEquals(doc1.toString(), bulkByScrollResponse.getGetResultsOld().get(1).sourceAsString());
            }
            doc1.setCounter(doc1.counter+5);
            doc2.setCounter(doc2.counter+5);
        }
        {
            // update by query and fetch new version of document
            BulkByScrollResponse bulkByScrollResponse = DocumentFetch.updateByQueryDocument(sourceIndex, client,
                    new Script("ctx._source.counter+=5"),
                    new TermQueryBuilder("counter",7),
                    true,false);

            assertNotEquals(null, bulkByScrollResponse.getGetResults());
            doc1.setCounter(doc1.counter+5);
            doc2.setCounter(doc2.counter+5);

            if (bulkByScrollResponse.getGetResults().get(0).getId().equals("2")) {
                assertEquals(doc1.toStringOrder2(), bulkByScrollResponse.getGetResults().get(0).sourceAsString());
                assertEquals(doc2.toStringOrder2(), bulkByScrollResponse.getGetResults().get(1).sourceAsString());
            } else {
                assertEquals(doc2.toStringOrder2(), bulkByScrollResponse.getGetResults().get(0).sourceAsString());
                assertEquals(doc1.toStringOrder2(), bulkByScrollResponse.getGetResults().get(1).sourceAsString());
            }
        }
    }
}
