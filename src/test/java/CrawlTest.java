import com.tallate.docindex.DocIndexApplication;
import com.tallate.docindex.bean.Page;
import com.tallate.docindex.bean.PageRequest;
import com.tallate.docindex.crawler.PageCrawler;
import com.tallate.docindex.crawler.PageIndexService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK, classes = DocIndexApplication.class)
public class CrawlTest {

    @Resource
    private PageCrawler pageCrawler;

    @Resource
    private PageIndexService pageIndexService;

    @Test
    public void testCrawlURL() throws InterruptedException {
        String url = "https://tallate.github.io/73d751b6.html";
        pageCrawler.crawl(url, 2);

        Thread.sleep(10000);
    }

    @Test
    public void testSearchPage() {
        PageRequest pageRequest = new PageRequest();
        pageRequest.setSearchKey("建立网页索引");
        List<Page> pages = pageIndexService.search(pageRequest);
        System.out.println("结果：");
        System.out.println(pages);
    }

}
