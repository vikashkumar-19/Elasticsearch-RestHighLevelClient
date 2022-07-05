package org.elasticsearch;


import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class DocumentFetchTest {
    private  RestHighLevelClient client;
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
        }
        {
            // insert document
            DocumentFetch.docModel doc = new DocumentFetch.docModel(10,"Green","A");
            IndexResponse indexResponse = DocumentFetch.InsertDocument(sourceIndex, "1", doc, client, true);
            assertEquals(doc.toString(), DocumentFetch.docModel.getStringForm(indexResponse.getGetResult().getSource()));
        }
    }

    @Test
    public void testUpdate() throws IOException{
        DocumentFetch.docModel doc = new DocumentFetch.docModel(1,"Green","A");
        Script script = new Script("ctx._source.counter+=5");
        {
            // prepare
            IndexResponse indexResponse = DocumentFetch.InsertDocument(sourceIndex, "1", doc, client);
        }
        {
            // update document and fetch old version
            UpdateResponse updateResponse = DocumentFetch.updateDocument(sourceIndex, "1", client, script, false, true);
            assertEquals(doc.toString(), DocumentFetch.docModel.getStringForm(updateResponse.getGetResultOld().getSource()));
            doc.setCounter(doc.counter+5);
        }
        {
            // update document and fetch new version
            UpdateResponse updateResponse = DocumentFetch.updateDocument(sourceIndex, "1", client, script, true, false);
            doc.setCounter(doc.counter+5);
            assertEquals(doc.toString(), DocumentFetch.docModel.getStringForm(updateResponse.getGetResult().getSource()));
        }
    }

    @Test
    public void testUpdateByQuery() throws IOException{
        DocumentFetch.docModel doc1 = new DocumentFetch.docModel(2,"Red","B");
        DocumentFetch.docModel doc2 = new DocumentFetch.docModel(2,"Brown","C");
        {
            // prepare
            DocumentFetch.InsertDocument(sourceIndex,"2",doc1,client);
            DocumentFetch.InsertDocument(sourceIndex,"3",doc2,client);
        }
        {
            // update by query and fetch old version of documents
            BulkByScrollResponse bulkByScrollResponse = DocumentFetch.updateByQueryDocument(sourceIndex, client,
                    new Script("ctx._source.counter+=5"),
                    new TermQueryBuilder("counter",2),
                    false,true);

            assertNotEquals(null, bulkByScrollResponse.getGetResultsOld());
            if (bulkByScrollResponse.getGetResultsOld().get(0).getId().equals("2")) {
                assertEquals(doc1.toString(), DocumentFetch.docModel.getStringForm(bulkByScrollResponse.getGetResultsOld().get(0).getSource()));
                assertEquals(doc2.toString(), DocumentFetch.docModel.getStringForm(bulkByScrollResponse.getGetResultsOld().get(1).getSource()));
            } else {
                assertEquals(doc2.toString(), DocumentFetch.docModel.getStringForm(bulkByScrollResponse.getGetResultsOld().get(0).getSource()));
                assertEquals(doc1.toString(), DocumentFetch.docModel.getStringForm(bulkByScrollResponse.getGetResultsOld().get(1).getSource()));
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
                assertEquals(doc1.toString(),DocumentFetch.docModel.getStringForm(bulkByScrollResponse.getGetResults().get(0).getSource()));
                assertEquals(doc2.toString(), DocumentFetch.docModel.getStringForm(bulkByScrollResponse.getGetResults().get(1).getSource()));
            } else {
                assertEquals(doc2.toString(), DocumentFetch.docModel.getStringForm(bulkByScrollResponse.getGetResults().get(0).getSource()));
                assertEquals(doc1.toString(), DocumentFetch.docModel.getStringForm(bulkByScrollResponse.getGetResults().get(1).getSource()));
            }
        }
    }

    @Test
    public void testUpdateByQueryMaxDocsReturn() throws IOException{
        List<DocumentFetch.docModel> docs = new ArrayList<>();
        for(int i=1;i<=15;i++){
            docs.add(new DocumentFetch.docModel(101,"Color","A"+i));
        }
        {
            //prepare
            for(int i=1;i<=15;i++){
                DocumentFetch.InsertDocument(sourceIndex,""+i,docs.get(i-1),client);
            }
        }
        {
            // update by query and max docs return with default value (= 10)
            BulkByScrollResponse bulkByScrollResponse = DocumentFetch.updateByQueryDocument(sourceIndex, client,
                    new Script("ctx._source.counter+=5"),
                    new TermQueryBuilder("counter",101),
                    true,true);

            assertNotEquals(null, bulkByScrollResponse.getGetResults());
            assertEquals(10, bulkByScrollResponse.getGetResults().size());
            assertEquals(10, bulkByScrollResponse.getGetResultsOld().size());
        }
        {
            // update by query and max docs return 12<15(total hits)

            UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(sourceIndex)
                    .setQuery(new TermQueryBuilder("counter",106))
                    .fetchSourceOld(true)
                    .fetchSource(true)
                    .setScript(new Script("ctx._source.counter+=5"))
                    .setMaxDocsReturn(12)
                    .setRefresh(true);
            BulkByScrollResponse bulkByScrollResponse = client.updateByQuery(updateByQueryRequest, DocumentFetch.prepareRequestOptions());

            assertNotEquals(null, bulkByScrollResponse.getGetResults());
            assertEquals(12, bulkByScrollResponse.getGetResults().size());
            assertEquals(12, bulkByScrollResponse.getGetResultsOld().size());
        }
        {
            // update by query and max docs return 12<15(total hits) and higher than default value

            UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(sourceIndex)
                    .setQuery(new TermQueryBuilder("counter",111))
                    .fetchSourceOld(true)
                    .fetchSource(true)
                    .setScript(new Script("ctx._source.counter+=5"))
                    .setMaxDocsReturn(6)
                    .setRefresh(true);
            BulkByScrollResponse bulkByScrollResponse = client.updateByQuery(updateByQueryRequest, DocumentFetch.prepareRequestOptions());

            assertNotEquals(null, bulkByScrollResponse.getGetResults());
            assertEquals(6, bulkByScrollResponse.getGetResults().size());
            assertEquals(6, bulkByScrollResponse.getGetResultsOld().size());
        }
    }
}
