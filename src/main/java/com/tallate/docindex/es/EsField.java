package com.tallate.docindex.es;

import java.lang.annotation.*;

@Documented
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface EsField {

  boolean include() default true;

}
