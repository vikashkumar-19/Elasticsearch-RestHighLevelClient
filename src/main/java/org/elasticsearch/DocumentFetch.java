package org.elasticsearch;


import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;


import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class DocumentFetch {
    private static final String AUTH ="Basic ZWxhc3RpYy1hZG1pbjplbGFzdGljLXBhc3N3b3Jk";

    public static IndexResponse InsertDocument(String sourceIndex, String id, docModel doc, RestHighLevelClient client) throws IOException {
        return InsertDocument(sourceIndex, id, doc, client, false);
    }
    public static IndexResponse InsertDocument(String sourceIndex, String id, docModel doc, RestHighLevelClient client, boolean fetchSource) throws IOException {
        IndexRequest request = new IndexRequest(sourceIndex)
                .id(id)
                .source(doc.GetXContent())
                .fetchSource(fetchSource)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse response = client.index(request, prepareRequestOptions());
        return response;
    }

    public static UpdateResponse updateDocument(String sourceIndex, String id, RestHighLevelClient client, Script script,
                                                boolean fetchSource, boolean fetchsourceOld) throws IOException {
        UpdateRequest request = new UpdateRequest(sourceIndex, id)
                .script(script)
                .fetchSource(fetchSource)
                .fetchSourceOld(fetchsourceOld)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        UpdateResponse response = client.update(request, prepareRequestOptions());
        return response;
    }

    public static BulkByScrollResponse updateByQueryDocument(String sourceIndex, RestHighLevelClient client,
                                                             Script script, TermQueryBuilder termQueryBuilder,
                                                             boolean fetchSource, boolean fetchSourceOld) throws IOException {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(sourceIndex)
                .setQuery( termQueryBuilder)
                .fetchSourceOld(fetchSourceOld)
                .fetchSource(fetchSource)
                .setScript(script)
                .setRefresh(true);
        return client.updateByQuery(updateByQueryRequest, prepareRequestOptions());
    }
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient( RestClient.builder(
                new HttpHost("localhost", 9200, "http")));
        insertDocument(client);
        testIndex(client);
        testUpdate(client);
        testUpdateByQuery(client);
        client.close();
    }
    public static void insertDocument(RestHighLevelClient client) throws IOException {
        String sourceIndex="index_test";
        docModel doc1 = new docModel(1,"Green","A");
        docModel doc2 = new docModel(2,"Red","B");
        docModel doc3 = new docModel(2,"Brown","C");

        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest(sourceIndex).id("1").source(doc1.GetXContent()));
        request.add(new IndexRequest(sourceIndex).id("2").source(doc2.GetXContent()));
        request.add(new IndexRequest(sourceIndex).id("3").source(doc3.GetXContent()));
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        assertEquals(RestStatus.OK,client.bulk(request,prepareRequestOptions()).status());
    }
    public static void testIndex(RestHighLevelClient client) throws IOException {
        String sourceIndex="index_test";
        docModel doc1 = new docModel(1,"Green","A");
        doc1.setCounter(10);

        IndexRequest indexRequest = new IndexRequest(sourceIndex).id("1").source(doc1.GetXContent())
                .fetchSource(new FetchSourceContext(true))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        IndexResponse res = client.index(indexRequest,prepareRequestOptions());
//        System.out.println(res.getGetResult());
        assertEquals(doc1.toString(),res.getGetResult().sourceAsString());
    }
    public static void testUpdate(RestHighLevelClient client) throws IOException {
        String sourceIndex="index_test";

        GetResponse getResponse = client.get(new GetRequest(sourceIndex,"1"),prepareRequestOptions());
//        System.out.println(getResponse.getSource());
        docModel doc = new docModel(
                (int)getResponse.getSource().get("counter"),
                (String)getResponse.getSource().get("tag"),
                (String)getResponse.getSource().get("owner"));

        UpdateRequest updateRequest1 = new UpdateRequest(sourceIndex,"1")
                .script(new Script("ctx._source.counter++"))
                .fetchSourceOld(true)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        UpdateResponse updateRes1 = client.update(updateRequest1,prepareRequestOptions());
        assertNotEquals(null,updateRes1.getGetResultOld());
        assertEquals(doc.toString(),updateRes1.getGetResultOld().sourceAsString());
        doc.setCounter(doc.counter+2);

        UpdateRequest updateRequest2 = new UpdateRequest(sourceIndex,"1")
                .script(new Script("ctx._source.counter++"))
                .fetchSource(true)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        UpdateResponse updateRes2 = client.update(updateRequest2,prepareRequestOptions());
        assertNotEquals(null,updateRes2.getGetResult());
        assertEquals(doc.toString(),updateRes2.getGetResult().sourceAsString());
    }
    public static void testUpdateByQuery(RestHighLevelClient client) throws IOException {
        String sourceIndex="index_test";

        docModel doc2 = new docModel(2,"Red","B");
        docModel doc3 = new docModel(2,"Brown","C");

        // old version test
        {
            UpdateByQueryRequest updateByQueryRequest1 = new UpdateByQueryRequest(sourceIndex)
                    .setQuery(new TermQueryBuilder("counter", 2))
                    .fetchSourceOld(new FetchSourceContext(true))
                    .setScript(new Script("ctx._source.counter=3"))
                    .setRefresh(true);


            BulkByScrollResponse res1 = client.updateByQuery(updateByQueryRequest1, prepareRequestOptions());

            assertNotEquals(null, res1.getGetResultsOld());
//            System.out.println(res1.getGetResultsOld().size());
            if (res1.getGetResultsOld().get(0).getId().equals("2")) {
                assertEquals(doc2.toString(), res1.getGetResultsOld().get(0).sourceAsString());
                assertEquals(doc3.toString(), res1.getGetResultsOld().get(1).sourceAsString());
            } else {
                assertEquals(doc3.toString(), res1.getGetResultsOld().get(0).sourceAsString());
                assertEquals(doc2.toString(), res1.getGetResultsOld().get(1).sourceAsString());
            }
        }


        {
            // new version test
            UpdateByQueryRequest updateByQueryRequest2 = new UpdateByQueryRequest(sourceIndex)
                    .setQuery(new TermQueryBuilder("counter", 3))
                    .fetchSource(new FetchSourceContext(true))
                    .setScript(new Script("ctx._source.counter=2"))
                    .setRefresh(true);

            BulkByScrollResponse res2 = client.updateByQuery(updateByQueryRequest2, prepareRequestOptions());
            assertNotEquals(null, res2.getGetResults());
//            System.out.println(res2.getGetResults().size());
            if (res2.getGetResults().get(0).getId().equals("2")) {
                // order of fields changed, so used dif method to return string form.
                assertEquals(doc2.toStringOrder2(), res2.getGetResults().get(0).sourceAsString());
                assertEquals(doc3.toStringOrder2(), res2.getGetResults().get(1).sourceAsString());
            } else {
                assertEquals(doc3.toStringOrder2(), res2.getGetResults().get(0).sourceAsString());
                assertEquals(doc2.toStringOrder2(), res2.getGetResults().get(1).sourceAsString());
            }
        }
    }


    public static RequestOptions prepareRequestOptions(){
        RequestOptions.Builder rqq = RequestOptions.DEFAULT.toBuilder();
        rqq.addHeader(HttpHeaders.AUTHORIZATION,AUTH)
                .addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        return rqq.build();
    }

    public static class docModel {
        public int counter;
        public String tag;
        public String owner;

        public docModel(int counter, String tag, String owner) {
            this.counter = counter;
            this.tag = tag;
            this.owner = owner;
        }

        public XContentBuilder GetXContent() throws IOException {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field("counter", counter);
                builder.field("tag",tag);
                builder.field("owner", owner);
            }
            builder.endObject();
            return builder;
        }

        public int getCounter() {
            return counter;
        }

        public void setCounter(int counter) {
            this.counter = counter;
        }

        public void setOwner(String owner) {
            this.owner = owner;
        }

        @Override
        public String toString() {
            return "{" +
                    "\"counter\":" + counter +
                    ",\"tag\":\"" + tag + '\"' +
                    ",\"owner\":\"" + owner + '\"' +
                    '}';
        }
        public String toStringOrder2(){
            return "{" +
                    "\"owner\":\"" + owner + '\"' +
                    ",\"counter\":" + counter +
                    ",\"tag\":\"" + tag + '\"' +
                    '}';
        }
    }
}