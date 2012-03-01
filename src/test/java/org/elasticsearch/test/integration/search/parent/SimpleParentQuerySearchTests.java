/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.integration.search.parent;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.elasticsearch.index.query.FilterBuilders.hasParentFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.facet.FacetBuilders.termsFacet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleParentQuerySearchTests extends AbstractNodesTests {

    private final String INDEX = "test";

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("node1");
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node1");
    }

    @Test
    public void simpleParentQuery() throws Exception {
        initializeCluster();
        client.admin().indices().preparePutMapping(INDEX).setType("child").setSource(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "parent").endObject()
                .endObject().endObject()).execute().actionGet();

        // index simple data
        client.prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1", "p_field2", 1).execute().actionGet();
        client.prepareIndex(INDEX, "child", "c1").setSource("c_field", "red").setParent("p1").execute().actionGet();
        client.prepareIndex(INDEX, "child", "c2").setSource("c_field", "yellow").setParent("p1").execute().actionGet();
        client.prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2", "p_field2", 1).execute().actionGet();
        client.prepareIndex(INDEX, "child", "c3").setSource("c_field", "blue").setParent("p2").execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse;

        // HAS PARENT FILTER

        searchResponse = client.prepareSearch(INDEX).setQuery(constantScoreQuery(hasParentFilter("parent", termQuery("p_field", "p_value1")))).execute().actionGet();
        assertThat("Failures " + Arrays.toString(searchResponse.shardFailures()), searchResponse.shardFailures().length, equalTo(0));
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("c1"), equalTo("c2")));

        searchResponse = client.prepareSearch(INDEX).setQuery(constantScoreQuery(hasParentFilter("parent", termQuery("p_field", "p_value_bad")))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(0L));

        searchResponse = client.prepareSearch(INDEX).setQuery(constantScoreQuery(hasParentFilter("parent", termQuery("p_field", "p_value2")))).execute().actionGet();
        assertThat("Failures " + Arrays.toString(searchResponse.shardFailures()), searchResponse.shardFailures().length, equalTo(0));
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("c3"));

        searchResponse = client.prepareSearch("test").setQuery(constantScoreQuery(hasParentFilter("parent", termQuery("p_field2", 1)))).execute().actionGet();
        assertThat("Failures " + Arrays.toString(searchResponse.shardFailures()), searchResponse.shardFailures().length, equalTo(0));
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(3l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("c1"), equalTo("c2"), equalTo("c3")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("c1"), equalTo("c2"), equalTo("c3")));
        assertThat(searchResponse.hits().getAt(2).id(), anyOf(equalTo("c1"), equalTo("c2"), equalTo("c3")));
    }

    @Test
    public void simpleParentQuery2() throws Exception {
        initializeCluster();
        String parentSource = "{'changeset' : {'properties' : {'ixHostedDB' : {'type' : 'integer'}, 'sId' : {'type' : 'string'}}}}".replace('\'', '"');
        String childSource = "{'changesetrepo' : {'properties' : {'ixRepo' : {'type' : 'integer'}}, '_parent' : {'type' : 'changeset'}}}".replace('\'', '"');
        client.admin().indices().preparePutMapping(INDEX)
            .setType("changeset")
            .setSource(parentSource)
            .execute()
            .actionGet();
        client.admin().indices().preparePutMapping(INDEX)
            .setType("changesetrepo")
            .setSource(childSource)
            .execute()
            .actionGet();

        // Four parent documents: c1, c2, c3, c4.
        // c1 has two parents: cr1 and cr2.
        // c4 has two parents: cr1 and cr2.
        client.prepareIndex(INDEX, "changeset", "c1")
            .setSource("ixHostedDB", -1, "sId", "095520ef2aaa5d1e14d46296fdb2464f9b21dcaf")
            .execute().actionGet();
        client.prepareIndex(INDEX, "changeset", "c2")
            .setSource("ixHostedDB", -2, "sId", "095520ef2aaa5d1e14d46296fdb2464f9b21dcaf")
            .execute().actionGet();
        client.prepareIndex(INDEX, "changeset", "c3")
            .setSource("ixHostedDB", -1, "sId", "095520ef2aaa5d1e14d46296fdb2464f9b21dcafx")
            .execute().actionGet();
        client.prepareIndex(INDEX, "changeset", "c4")
            .setSource("ixHostedDB", -1, "sId", "095520ef2aaa5d1e14d46296fdb2464f9b21dcaf")
            .execute().actionGet();
        client.prepareIndex(INDEX, "changesetrepo", "cr1")
            .setSource("ixRepo", 330)
            .setParent("c1")
            .execute().actionGet();
        client.prepareIndex(INDEX, "changesetrepo", "cr2")
            .setSource("ixRepo", 331)
            .setParent("c1")
            .execute().actionGet();
        client.prepareIndex(INDEX, "changesetrepo", "cr3")
            .setSource("ixRepo", 330)
            .setParent("c4")
            .execute().actionGet();
        client.prepareIndex(INDEX, "changesetrepo", "cr4")
            .setSource("ixRepo", 331)
            .setParent("c4")
            .execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse;
        String query, filter;

        // Both queries should return cr1 and cr4.
        query = "{'term':{'ixRepo':330}}".replace('\'', '"');
        filter = "{'has_parent':{'type':'changeset', 'query':{'term':{'sId':'095520ef2aaa5d1e14d46296fdb2464f9b21dcaf'}}}}".replace('\'', '"');
        searchResponse = client.prepareSearch(INDEX)
                .setQuery(query)
                .setSize(2000)
                .setFilter(filter)
                .execute().actionGet();
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(2L));

        filter = "{'has_parent':{'type':'changeset', 'query':{'term':{'ixHostedDB':-1}}}}".replace('\'', '"');
        searchResponse = client.prepareSearch(INDEX)
                .setQuery(query)
                .setSize(2000)
                .setFilter(filter)
                .execute().actionGet();
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(2L));
    }

    private void initializeCluster() {
        client.admin().indices().prepareDelete().execute().actionGet();
        client.admin().indices().prepareCreate(INDEX).setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
    }
}
