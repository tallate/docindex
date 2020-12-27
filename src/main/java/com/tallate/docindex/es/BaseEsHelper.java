package com.tallate.docindex.es;

import com.tallate.docindex.util.EsException;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Es工具基类
 * 1. 作为组件，提供一些参数
 *
 * @author hgc
 */
@Slf4j
public abstract class BaseEsHelper {

///////////////////////////////// 集群配置 /////////////////////////////////
//#search.nodes=140.143.13.189, localhost
//#search.ports=9200, 9200
//#search.number_of_shards=3
//#search.number_of_replicas=2
//#search.timeout=2m
//#search.master_timeout=1m
//#search.wait_for_active_shards=2
//#search.refresh_policy=wait_for

    ///////////////////////////////// 单机配置 /////////////////////////////////
    @Value("${search.nodes:127.0.0.1}")
    protected String[] nodes;
    @Value("${search.ports:9200}")
    protected int[] ports;
    @Value("${search.connection.schema:http}")
    protected String schema;
    @Value("${search.number_of_shards:3}")
    protected int number_of_shards;
    @Value("${search.number_of_replicas:1}")
    protected int number_of_replicas;

    /**
     * 请求超时
     */
    @Value("${search.timeout:2m}")
    protected String timeout;

    /**
     * 等待主节点延迟
     */
    @Value("${search.master_timeout:1m}")
    protected String master_timeout;

    /**
     * 等待可用主分片的拷贝数
     * 每个主分片可以有多个副本分片
     * 只需要主分片ok就行，因为副本分片不能和主分片存在于同一节点
     */
    @Value("${search.wait_for_active_shards:1}")
    protected int wait_for_active_shards;

    /**
     * 刷新策略
     * 有以下两种设置方式：
     * request.setRefreshPolicy("wait_for"); // 一直开启写请求直到刷新
     * request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
     */
    @Value("${search.refresh_policy:wait_for}")
    protected String refresh_policy;

    protected RestHighLevelClient getClient() {
        if (null == nodes || nodes.length != ports.length) {
            throw new EsException("未指定es节点 或 es节点和端口号数不匹配");
        }
        List<HttpHost> hostList = new ArrayList<>();
        for (int i = 0; i < nodes.length; i++) {
            hostList.add(new HttpHost(nodes[i], ports[i], schema));
        }
        return new RestHighLevelClient(
                RestClient.builder(hostList.toArray(new HttpHost[0])));
    }

    public EsRequest getEsRequest() {
        EsRequest request = new EsRequest();
        request.setNumberOfShards(number_of_shards);
        request.setNumberOfReplicas(number_of_replicas);
        request.setTimeout(timeout);
        request.setMasterTimeout(master_timeout);
        request.setWaitForActiveShards(wait_for_active_shards);
        request.setRefreshPolicy(refresh_policy);
        // request.setVersion(1);
        return request;
    }

}
