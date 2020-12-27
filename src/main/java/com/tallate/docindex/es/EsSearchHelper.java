package com.tallate.docindex.es;

import com.tallate.docindex.es.convert.EsDoc2ObjUtil;
import com.tallate.docindex.util.EsException;
import com.tallate.docindex.util.UtilException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Component
public class EsSearchHelper extends BaseEsHelper {

    private SearchRequest getSearchRequest(EsRequest esRequest) throws EsException {
        SearchRequest searchRequest = new SearchRequest(esRequest.getIdxName());
        searchRequest.types(esRequest.getType());
//    searchRequest.routing(esRequest.getRouting());
        // TODO: 这面这些参数感觉没有什么自定义的必要
//    searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
//    searchRequest.preference("_local");
        searchRequest.source(esRequest.getSearchSourceBuilder());

        return searchRequest;
    }

    private <T> List<T> convertResults(SearchHit[] searchHits, EsRequest esRequest, Class<T> entityType) throws EsException, UtilException {
        try {
            // 先将所有source转换为entityType类型的对象
            List<T> entityList = new ArrayList<>();
            for (SearchHit searchHit : searchHits) {
                Map<String, Object> fieldMap = searchHit.getSourceAsMap();
                T entity = EsDoc2ObjUtil.convert(fieldMap, esRequest.getIncludes(), entityType);
                entityList.add(entity);
            }
            return entityList;
        } catch (IllegalAccessException | InstantiationException e) {
            throw new UtilException("反序列化对象失败", e);
        }
    }

    public List<Map<String, Object>> search(EsRequest esRequest) {
        try (RestHighLevelClient client = getClient()) {
            SearchRequest request = getSearchRequest(esRequest);
            log.info("搜索请求, requestBody:{}", request);
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            SearchHit[] hits = response.getHits().getHits();
            return Arrays.stream(hits)
                    .map(SearchHit::getSourceAsMap)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UtilException("获取文档请求失败", e);
        }
    }

    public <T> List<T> search(EsRequest esRequest, Class<T> entityType, ActionListener<SearchResponse> listener) throws EsException, UtilException {
        try (RestHighLevelClient client = getClient()) {
            SearchRequest request = getSearchRequest(esRequest);
            if (null == listener) {
                SearchResponse response = client.search(request);
                RestStatus status = response.status();
                TimeValue took = response.getTook();
                Boolean terminatedEarly = response.isTerminatedEarly();
                boolean timedOut = response.isTimedOut();
                // 检查返回值
                int totalShards = response.getTotalShards();
                int successfulShards = response.getSuccessfulShards();
                int failedShards = response.getFailedShards();
                StringBuilder errSb = new StringBuilder();
                for (ShardSearchFailure failure : response.getShardFailures()) {
                    errSb.append(failure.reason());
                }
                if (StringUtils.isNotBlank(errSb.toString())) {
                    throw new EsException("在某些分片上查询出错：" + errSb);
                }
                return convertResults(response.getHits().getHits(), esRequest, entityType);
            } else {
                client.searchAsync(request, listener);
            }
            return null;
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                throw new UtilException("索引不存在", e);
            } else if (e.status() == RestStatus.CONFLICT) {
                throw new UtilException("文档已过期，请刷新", e);
            }
            throw new UtilException("获取文档请求失败", e);
        } catch (IOException e) {
            throw new UtilException("获取文档请求失败", e);
        }
    }

}
