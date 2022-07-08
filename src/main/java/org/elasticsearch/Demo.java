package org.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;

public class Demo {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient( RestClient.builder(
                new HttpHost("localhost", 9200, "http")));
        Update(client);
        client.close();
    }

    public static void Update(RestHighLevelClient client) throws IOException {
        String sourceIndex = "demo_index";

        {
            // prepare
            IndexRequest request = new IndexRequest(sourceIndex)
                    .source( new DocumentFetch.docModel(2,"Green","A").GetXContent())
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

            IndexResponse res = client.index(request, DocumentFetch.prepareRequestOptions());
            System.out.println(res.getId());
        }
        {
            // fetch new version
            UpdateRequest request= new UpdateRequest(sourceIndex,"1")
                    .script(new Script("ctx._source.counter++"))
                    .fetchSource(true)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            // execute
            UpdateResponse response = client.update(request, DocumentFetch.prepareRequestOptions());
            System.out.println(response.getGetResult());
        }
    }
}
