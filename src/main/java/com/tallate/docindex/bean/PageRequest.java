package com.tallate.docindex.bean;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

@Data
@ToString
public class PageRequest implements Serializable {

    private String url;

    private String searchKey;

    private int depth;

}
