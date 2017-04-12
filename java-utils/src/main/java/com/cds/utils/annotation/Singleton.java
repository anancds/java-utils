package com.cds.utils.annotation;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import org.elasticsearch.common.inject.ScopeAnnotation;

/**
 * Created by chendongsheng5 on 2017/4/12.
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RUNTIME)
@ScopeAnnotation
public @interface Singleton {
}
