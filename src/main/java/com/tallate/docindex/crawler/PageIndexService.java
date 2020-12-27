package com.tallate.docindex.crawler;

import com.google.common.collect.Maps;
import com.tallate.docindex.bean.Page;
import com.tallate.docindex.bean.PageRequest;
import com.tallate.docindex.es.EsRequest;
import com.tallate.docindex.es.EsSearchHelper;
import com.tallate.docindex.es.EsSingleDocHelper;
import com.tallate.docindex.util.TypeResolveUtil;
import org.assertj.core.util.Lists;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.tallate.docindex.bean.PageConstant.*;

@Service
public class PageIndexService {

    @Resource
    private EsSingleDocHelper esSingleDocHelper;

    @Resource
    private EsSearchHelper esSearchHelper;

    private EsRequest toIndexRequest(Page page) {
        EsRequest request = new EsRequest();
        request.setIdxName(PAGE_INDEX_NAME);
        request.setType(PAGE_TYPE_NAME);
        request.setDocId(page.getId());
        Map<String, Object> map = Maps.newHashMap();
        map.put(URL, page.getUrl());
        map.put(TITLE, page.getTitle());
        map.put(CONTENT, page.getContent());
        map.put(AS, page.getAs());
        request.setSource(map);
        return request;
    }

    public void index(Page page) {
        EsRequest request = toIndexRequest(page);
        esSingleDocHelper.updateDocument(request);
    }

    private EsRequest toSearchRequest(PageRequest pageRequest) {
        EsRequest request = new EsRequest();
        request.setIdxName(PAGE_INDEX_NAME);
        request.setType(PAGE_TYPE_NAME);
        request.setSearchSourceBuilder(SearchSourceBuilder.searchSource()
                .fetchSource(true)
                .from(0)
                .size(100)
                .query(QueryBuilders.boolQuery()
                        // 还是更倾向于搜索标题
                        .should(QueryBuilders.multiMatchQuery(pageRequest.getSearchKey(), TITLE).boost(3))
                        .should(QueryBuilders.multiMatchQuery(pageRequest.getSearchKey(), CONTENT).boost(1))));
        // 以下是简单搜索content字段的查询条件
        // .query(QueryBuilders.matchQuery(CONTENT, pageRequest.getSearchKey()))
        return request;
    }

    public List<Page> search(PageRequest pageRequest) {
        EsRequest request = toSearchRequest(pageRequest);
        List<Map<String, Object>> result = Optional.ofNullable(esSearchHelper.search(request)).orElse(Lists.newArrayList());
        Stream<Page> pageStream = result.stream()
                .map(map -> {
                    Page page = new Page();
                    page.setUrl(TypeResolveUtil.resolve(map.get(URL), String.class));
                    page.setTitle(TypeResolveUtil.resolve(map.get(TITLE), String.class));
                    page.setContent(TypeResolveUtil.resolve(map.get(CONTENT), String.class));
                    page.setAs(TypeResolveUtil.resolve(map.get(AS), List.class));
                    return page;
                });
        return pageStream
                .collect(Collectors.toList());
    }

}
