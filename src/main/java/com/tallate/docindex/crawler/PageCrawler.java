package com.tallate.docindex.crawler;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.tallate.docindex.bean.Page;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Lists;
import org.assertj.core.util.Sets;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Slf4j
@Component
public class PageCrawler extends Crawler {

    /**
     * 需要被过滤掉的无效url
     */
    private Set<String> excludeUrls = Sets.newHashSet(Lists.newArrayList("", "/", "javascript:;"));

    /**
     * 按后缀过滤url
     */
    private List<String> excludeEnds = Lists.newArrayList(".svg", ".xml", ".ico", ".jpg", ".jpeg", ".png");

    /**
     * 按标签过滤页面中的内容
     */
    private Set<String> excludeTags = Sets.newHashSet(Lists.newArrayList("script", "style", "html", "head", "body", "textarea"));

    private LoadingCache<String, ConcurrentMap<String, String>> traversed = CacheBuilder.newBuilder()
            .maximumSize(500)
            .expireAfterWrite(1, TimeUnit.MINUTES)
            .build(new CacheLoader<String, ConcurrentMap<String, String>>() {
                @Override
                public ConcurrentMap<String, String> load(String key) throws Exception {
                    return Maps.newConcurrentMap();
                }
            });

    private ExecutorService threadPool = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2 + 1,
            Runtime.getRuntime().availableProcessors() * 2 + 1,
            30, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1000),
            runnable -> new Thread(runnable, "crawler"),
            new ThreadPoolExecutor.CallerRunsPolicy());

    @Resource
    private PageIndexService pageIndexService;

    private Document getRootDoc(String url) {
        try {
            return Jsoup.connect(url).get();
        } catch (IOException e) {
            log.info("忽略连接异常, url:{}, error:{}", url, e.getMessage());
        } catch (Exception e) {
            log.error("忽略请求url异常, url:{}, error:{}", url, e.getMessage());
        }
        return null;
    }

    public List<String> getChildUrls(Element parent, String urlTag) {
        Elements as = parent.getElementsByTag(urlTag);
        List<String> subUrls = Lists.newArrayList();
        for (Element a : as) {
            String subUrl = a.attr("href");
            subUrls.add(subUrl);
        }
        return subUrls;
    }

    private boolean isUrlValid(String crawlId, String url) {
        // 过滤无效的url
        if (excludeUrls.contains(url)) {
            return false;
        }
        // 过滤无效的后缀名
        for (String excludeEnd : excludeEnds) {
            if (url.endsWith(excludeEnd)) {
                return false;
            }
        }
        // 不需要太严格的防重，一个页面重复拉两次也不会有太大问题
        ConcurrentMap<String, String> traversedMap = null;
        try {
            traversedMap = traversed.get(crawlId);
        } catch (ExecutionException e) {
            log.error("获取已读url缓存失败, 继续拉取, url:{}", url, e);
            return true;
        }
        if (traversedMap.containsKey(url)) {
            return false;
        }
        traversedMap.put(url, "1");
        return true;
    }

    private String fixUrl(String url) {
        // 修复链接格式
        if (url.startsWith("//")) {
            return "http:" + url;
        } else if (url.startsWith("/")) {
            return "http:/" + url;
        }
        return url;
    }

    @AllArgsConstructor
    private class CrawlTask implements Runnable {

        private String crawlId;

        private String url;

        private int depth;

        @Override
        public void run() {
            if (depth <= 0) {
                return;
            }
            log.info("开始处理url:{}", url);
            // 修复url
            url = fixUrl(url);
            Document doc = getRootDoc(url);
            if (null == doc) {
                return;
            }
            Page page = new Page(url);
            // 标题
            String title = doc.title();
            page.setTitle(title);
            // 页面文本内容
            Elements allElements = doc.getAllElements();
            StringBuilder contentBuilder = new StringBuilder();
            for (Element e : allElements) {
                // 一些特殊标签忽略，比如style这种
                if (excludeTags.contains(e.tag().getName())) {
                    continue;
                }
                contentBuilder.append(" ").append(e.ownText());
            }
            page.setContent(contentBuilder.toString());
            // 递归访问子链接
            Set<String> childUrls = Sets.newHashSet();
            Elements elements = doc.getAllElements();
            for (Element element : elements) {
                List<String> as = getChildUrls(element, "a");
                List<String> links = getChildUrls(element, "link");
                childUrls.addAll(as);
                childUrls.addAll(links);
            }
            // 过滤无效的
            childUrls = childUrls.stream()
                    .filter(u -> isUrlValid(crawlId, u))
                    .collect(Collectors.toSet());
            log.info("找出的子链接, childUrls:{}", childUrls);
            page.getAs().addAll(childUrls);
            for (String childUrl : childUrls) {
                // 递归爬
                threadPool.execute(new CrawlTask(crawlId, childUrl, depth - 1));
            }
            log.info("新拉取页面, pageUrl:{}", url);
            pageIndexService.index(page);
            log.info("建索引成功, pageUrl:{}", url);
        }
    }

    private String randomCrawlId() {
        return UUID.randomUUID().toString();
    }

    @Override
    public void crawl(String url, int depth) {
        String crawlId = randomCrawlId();
        if (!isUrlValid(crawlId, url)) {
            return;
        }
        threadPool.execute(new CrawlTask(crawlId, url, depth));
    }

}
