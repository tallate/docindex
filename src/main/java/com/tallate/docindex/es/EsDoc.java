package com.tallate.docindex.es;

import java.lang.annotation.*;

@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface EsDoc {

  String index();

  String mapping();

  String id() default "id";

  String version() default "version";

}
