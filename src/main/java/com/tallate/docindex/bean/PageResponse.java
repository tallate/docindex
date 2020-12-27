package com.tallate.docindex.bean;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
@Builder
public class PageResponse implements Serializable {

    private List<Page> pages;

}
