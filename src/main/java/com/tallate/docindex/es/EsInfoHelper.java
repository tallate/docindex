package com.tallate.docindex.es;

import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * 集群信息获取工具
 *
 * @author hgc
 */
@Component
public class EsInfoHelper extends BaseEsHelper {

    /**
     * 获取集群信息
     */
    public MainResponse getInfo() throws IOException {
        RestHighLevelClient client = getClient();
        return client.info(RequestOptions.DEFAULT);
    }

}
