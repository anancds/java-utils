package com.cds.utils.es;

import com.cds.utils.StringUtils;
import com.cds.utils.hbase.HBaseDataSourceUtil;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.client.http.JestHttpClient;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.spark.SparkContext;
import org.elasticsearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by chendongsheng5 on 2017/3/30.
 */
public class IndexerServers {
  private static Logger LOG = LoggerFactory.getLogger(IndexerServers.class);
  public static Cache<String, Client> cachedEsClient = CacheBuilder.newBuilder().
      expireAfterAccess(1, TimeUnit.DAYS).removalListener(new EsClientRemoveListener()).build();

//  public static SolrServer getSolrIndexerServer(String indexerUrl, SparkContext sc) throws Exception {
//
//    if (!StringUtils.hasText(indexerUrl)) {
//      throw new Exception("The indexerURL can not be null.");
//    }
//    String[] zkParams = zkUrlParse(indexerUrl);
//    if (null != zkParams) {
//      String zkHost = zkParams[0];
//      String defaultCollection = zkParams[1];
//      CloudSolrServer cloudSolrServer = new CloudSolrServer(zkHost);
//      final int zkClientTimeout = sc.conf().getInt(HBaseDataSourceUtil.INDEX_ZK_CLIENT_TIMEOUT,
//          HBaseDataSourceUtil.INDEX_ZK_CLIENT_TIMEOUT_DEFAULT);
//      final int zkConnectTimeout = sc.conf().getInt(HBaseDataSourceUtil.INDEX_ZK_CONNECT_TIMEOUT,
//          HBaseDataSourceUtil.INDEX_ZK_CONNECT_TIMEOUT_DEFAULT);
//      cloudSolrServer.setDefaultCollection(defaultCollection);
//      cloudSolrServer.setZkClientTimeout(zkClientTimeout);
//      cloudSolrServer.setZkConnectTimeout(zkConnectTimeout);
//
//      return cloudSolrServer;
//    } else if (indexerUrl.startsWith(HBaseDataSourceUtil.INDEX_URL_HTTP_PREFIX)) {
//
//      return new HttpSolrServer(indexerUrl);
//    } else {
//      throw new Exception("The indexerURL is invalid. indexerURL = " + indexerUrl);
//    }
//  }
//
//  public static SolrServer getSolrIndexerServer(String indexerUrl, int zkClientTimeout, int zkConnectTimeout, int maxBuffer) throws Exception {
//
//    if (!StringUtils.hasText(indexerUrl)) {
//      throw new Exception("The indexerURL can not be null.");
//    }
//    String[] zkParams = zkUrlParse(indexerUrl);
//    if (null != zkParams) {
//      String zkHost = zkParams[0];
//      String defaultCollection = zkParams[1];
//      System.setProperty("jute.maxbuffer", String.valueOf(maxBuffer));
//      CloudSolrServer cloudSolrServer = new CloudSolrServer(zkHost);
//      cloudSolrServer.setDefaultCollection(defaultCollection);
//      cloudSolrServer.setZkClientTimeout(zkClientTimeout);
//      cloudSolrServer.setZkConnectTimeout(zkConnectTimeout);
//
//      return cloudSolrServer;
//    } else if (indexerUrl.startsWith(HBaseDataSourceUtil.INDEX_URL_HTTP_PREFIX)) {
//
//      return new HttpSolrServer(indexerUrl);
//    } else {
//      throw new Exception("The indexerURL is invalid. indexerURL = " + indexerUrl);
//    }
//  }

  public static Tuple2<SearchRequestBuilder, Client> getEsServer(String esUrl,
      java.util.Set<String> relatedShards,
      final int esClientTimeout,
      final boolean esClientSniff,
      final String clusterName) throws Exception {
    final Tuple3<String[], String, String> tuple3 = parserESUrl(esUrl);
    Client client = cachedEsClient.get(Arrays.toString(tuple3._1()), new Callable<Client>() {
      @Override
      public Client call() throws Exception {
        TransportClient esClient = (TransportClient) originalESClient(tuple3._1(), esClientTimeout, esClientSniff, clusterName);
        if (esClient.connectedNodes().size() == 0) {
          esClient.close();
          throw new Exception("can not get es-connnect");
        }
        return esClient;
      }
    });
    String typeName = tuple3._3();
    String[] indexNames = new String[relatedShards.size()];
    int n = 0;
    for (String item : relatedShards) {
      indexNames[n] = item;
      n++;
    }

    SearchRequestBuilder builder = client.prepareSearch(indexNames).setTypes(typeName);
    return new Tuple2<SearchRequestBuilder, Client>(builder, client);
  }

  public static Client getEsClient(final String[] hostPair,
      final int esClientTimeout,
      final boolean esClientSniff,
      final String clusterName) throws IOException {
    Client client = null;
    try {
      client = cachedEsClient.get(Arrays.toString(hostPair), new Callable<Client>() {
        @Override
        public Client call() throws Exception {
          return originalESClient(hostPair, esClientTimeout, esClientSniff, clusterName);
        }
      });
    } catch (ExecutionException e) {
      throw new IOException("failed to get Es Client", e);
    }
    return client;
  }

  private static Client originalESClient(String[] hostPair,
      int esClientTimeout,
      boolean esClientSniff,
      String clusterName) throws IOException {
    LOG.debug("need rebuild es connection cache");
    TransportAddress[] addrs = new TransportAddress[hostPair.length];
    int i = 0;
    String[] keyValuePair;
    for (String t : hostPair) {
      keyValuePair = t.split(":");
      if (2 != keyValuePair.length) {
        throw new IOException("ES's host is not correct:" + Arrays.toString(keyValuePair));
      }
      addrs[i] = new InetSocketTransportAddress(InetAddress.getByName(keyValuePair[0]), Integer.valueOf(keyValuePair[1]));
      i++;
    }

    Settings settings = Settings.settingsBuilder()
        .put("cluster.name", clusterName)
        .put("client.transport.sniff", esClientSniff)
        .put("client.transport.ping_timeout", esClientTimeout + "s").build();
    return TransportClient.builder().settings(settings).build().addTransportAddresses(addrs);
  }

  private static class EsClientRemoveListener implements RemovalListener<String, Client> {
    @Override
    public void onRemoval(RemovalNotification<String, Client> notification) {
      if (null != notification && null != notification.getValue()) {
        LOG.debug("remove es cache" + notification.getKey());
        notification.getValue().close();
      }
    }
  }

  public static JestHttpClient getJestClient(String[] hostPair, int esClientTimeout, boolean esClientSniff, String clusterName) throws IOException {
    JestClientFactory factory = new JestClientFactory();
    List<String> restServerUrls = new ArrayList<>();
    String[] keyValuePair;
    for (String t : hostPair) {
      keyValuePair = t.split(":");
      if (2 != keyValuePair.length) {
        throw new IOException("ES's host is not correct:" + Arrays.toString(keyValuePair));
      }
      restServerUrls.add("http://" + keyValuePair[0] + ":" + "9200");
    }
    factory.setHttpClientConfig(new HttpClientConfig.Builder(
        restServerUrls).readTimeout(esClientTimeout * 1000).connTimeout(esClientTimeout * 1000).multiThreaded(true).discoveryEnabled(true).build());
    return (JestHttpClient) factory.getObject();
  }

  /**
   * 返回Collection名称
   */
  public static String parserSolrUrl(String solrUrl) throws IOException {

    // solr索引zk地址方式格式：zk:hdh137,hdh138,hdh140:testTable
    // solr索引http地址方式格式：http://hdh138:8984/solr/testTable
    if (!StringUtils.hasText(solrUrl)) {
      throw new IOException("The indexerURL can not be null.");
    }
    String[] zkParams = zkUrlParse(solrUrl);
    if (null != zkParams) {
      return zkParams[1];
    } else if (solrUrl.startsWith(HBaseDataSourceUtil.INDEX_URL_HTTP_PREFIX)) {

      String[] strings = solrUrl.split("/");
      return strings[strings.length - 1];
    } else {
      throw new IOException("The solrURL is invalid. indexerURL = " + solrUrl);
    }
  }

  public static Tuple3<String[], String, String> parserESUrl(String esUrl) throws IOException {

    // esUrl格式为:host:port,host:port:index/type
    String esHost;
    String[] indexType;
    try {
      int pos = esUrl.lastIndexOf(":");
      esHost = esUrl.substring(0, pos);
      indexType = esUrl.substring(pos + 1).split("/");
    } catch (Exception e) {
      throw new IOException("EsUrl is not in conformity with 'host:port,host:port:index/type'.esURL=" + esUrl);
    }
    if (2 != indexType.length) {
      throw new IOException("ES's index/type are not correct:" + esUrl);
    }

    if (!StringUtils.hasText(indexType[1]))
      throw new IOException("ES's type not be null:" + esUrl);

    String[] hostPair = esHost.split(",");
    if (1 > hostPair.length) {
      throw new IOException("ES's host is not correct:" + esHost);
    }

    return new Tuple3<String[], String, String>(hostPair, indexType[0], indexType[1]);
  }

  private static String[] zkUrlParse(String url) {

    Pattern pattern = Pattern.compile(HBaseDataSourceUtil.INDEX_URL_ZK_FORMAT);
    Matcher matcher = pattern.matcher(url);
    if (!matcher.find() || matcher.groupCount() != 2) {
      return null;
    }
    String[] result = new String[2];
    result[0] = matcher.group(1);
    result[1] = matcher.group(2);

    return result;
  }

  public static List<String> getAllIndexWithType(Client esClient, String indexPrefix, String indexType) throws IOException {
    GetIndexRequestBuilder indexSearch = esClient.admin().indices().prepareGetIndex().addTypes(indexType);
    GetIndexResponse searchResponse = indexSearch.execute().actionGet();
    String[] indexs = searchResponse.getIndices();
    List<String> relatedIndexs = new ArrayList<>();
    for (String index : indexs) {
      if (index.startsWith(indexPrefix + "-")) {
        relatedIndexs.add(index);
      }
    }
    return relatedIndexs;
  }

  public static long getTotalCountWithIndex(Client esClient, String index, String indexType, QueryBuilder queryBuilder) {
    SearchRequestBuilder builder = esClient.prepareSearch(index).setTypes(indexType).setQuery(queryBuilder);
    SearchResponse searchResponse = builder.execute().actionGet();
    return searchResponse.getHits().getTotalHits();
  }

  public static void main(String[] argv) {


    try {
      Tuple3<String[], String, String> tuple = parserESUrl("host:port,host:port:/typeName");
      String index = tuple._2();
      System.out.println(null == index);
      System.out.println(parserESUrl("host:port,host:port:/typeName"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
