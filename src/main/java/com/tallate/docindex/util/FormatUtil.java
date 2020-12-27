package com.tallate.docindex.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author hgc
 */
public class FormatUtil {

  public static Date parse(String dateStr) throws UtilException {
    try {
      DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
      return dateFormat.parse(dateStr);
    } catch (ParseException e) {
      throw new UtilException("parse date string [" + dateStr + "] failed");
    }
  }
}
