package com.tallate.docindex.bean;

import com.sun.org.apache.xml.internal.security.utils.Base64;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.assertj.core.util.Lists;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

@Data
@ToString
@AllArgsConstructor
public class Page {

    public Page() {
        id = UUID.randomUUID().toString();
    }

    public Page(String url) {
        id = Base64.encode(url.getBytes(StandardCharsets.UTF_8));
        this.url = url;
    }

    private String id;

    private String title;

    private String url;

    /**
     * 页面中所有文本内容
     */
    private String content;

    /**
     * 其他页面
     */
    private List<String> as = Lists.newArrayList();

}
