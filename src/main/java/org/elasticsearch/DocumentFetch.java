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