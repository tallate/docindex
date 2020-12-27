package com.tallate.docindex.controller;

import com.tallate.docindex.bean.Page;
import com.tallate.docindex.bean.PageRequest;
import com.tallate.docindex.bean.PageResponse;
import com.tallate.docindex.crawler.PageCrawler;
import com.tallate.docindex.crawler.PageIndexService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.List;

@RestController
@RequestMapping("/page")
public class PageCrawlController {

    @Resource
    private PageCrawler pageCrawler;

    @Resource
    private PageIndexService pageIndexService;

    @RequestMapping(value = "/crawl", method = RequestMethod.POST)
    @ResponseBody
    public String crawlWrite(@RequestBody PageRequest request) {
        pageCrawler.crawl(request.getUrl(), request.getDepth());
        return "success";
    }

    @RequestMapping(value = "/crawl", method = RequestMethod.GET)
    @ResponseBody
    public PageResponse crawlRead(@RequestBody PageRequest request) {
        List<Page> pages = pageIndexService.search(request);
        return PageResponse.builder()
                .pages(pages)
                .build();
    }

}
