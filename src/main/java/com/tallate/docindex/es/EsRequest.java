package com.tallate.docindex.es;

import lombok.Data;
import lombok.experimental.Accessors;
import org.elasticsearch.search.builder.SearchSourceBuilder;

@Data
@Accessors(chain = true)
public class EsRequest {

    private int numberOfShards;
    private int numberOfReplicas;
    private String timeout;
    private String masterTimeout;
    private int waitForActiveShards;
    // @see: WriteRequest.RefreshPolicy.WAIT_UNTIL
    private String refreshPolicy;

    // 基本查询参数
    private String idxName;
    private String type;
    private String docId;
    private String[] includes;
    private Object source;
    private long version;

    private String pipeline;

    private String routing;
    private String parent;

    /**
     * 由用户自己定义搜索条件
     */
    private SearchSourceBuilder searchSourceBuilder;


}
