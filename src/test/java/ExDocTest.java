import com.tallate.docindex.DocIndexApplication;
import com.tallate.docindex.es.EsInfoHelper;
import com.tallate.docindex.es.EsRequest;
import com.tallate.docindex.es.EsSingleDocHelper;
import com.tallate.docindex.util.UtilException;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Lists;
import org.elasticsearch.action.main.MainResponse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author hgc
 * @date 12/4/20
 */
@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK, classes = DocIndexApplication.class)
public class ExDocTest {

    @Resource
    private EsInfoHelper esInfoHelper;

    @Resource
    private EsSingleDocHelper esSingleDocHelper;

    @Test
    public void testInfo() throws IOException {
        MainResponse info = esInfoHelper.getInfo();
        log.info("clusterName:{}, nodeName:{}", info.getClusterName(), info.getNodeName());
    }

    @Test
    public void testCreateDoc() throws UtilException, InterruptedException {
        EsRequest esRequest = esSingleDocHelper.getEsRequest();
        esRequest.setIdxName("b");
        esRequest.setType("b");
        esRequest.setDocId("b");
        HashMap<Object, Object> source = new HashMap<>();
        source.put("x", "x");
        source.put("y", Lists.newArrayList());
        esRequest.setSource(source);
        esSingleDocHelper.updateDocument(esRequest);
        Thread.sleep(2000);
    }

    @Test
    public void testGetDoc() {
        EsRequest request = esSingleDocHelper.getEsRequest();
        request.setIdxName("b");
        request.setType("b");
        request.setDocId("b");
        Map<String, Object> document = esSingleDocHelper.getDocument(request);
        System.out.println(document);
    }

    @Test
    public void testUpdateDoc() {
        EsRequest esRequest = esSingleDocHelper.getEsRequest();
        esRequest.setIdxName("b");
        esRequest.setType("b");
        esRequest.setDocId("sasd");
        Map<String, String> map = new HashMap<>();
        map.put("name", "Mike");
        map.put("age", "18");
        map.put("article", "文章");
        esRequest.setSource(map);
        esSingleDocHelper.updateDocument(esRequest);
    }

}
