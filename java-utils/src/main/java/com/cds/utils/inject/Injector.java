package com.cds.utils.inject;

import java.util.List;
import org.elasticsearch.common.inject.Binding;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.MembersInjector;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.TypeLiteral;

/**
 * Created by chendongsheng5 on 2017/4/12.
 */
public interface Injector {

  void injectMembers(Object instance);

  <T> MembersInjector<T> getMembersInjector(TypeLiteral<T> typeLiteral);

  <T> MembersInjector<T> getMembersInjector(Class<T> type);

  <T> List<Binding<T>> findBindingsByType(TypeLiteral<T> type);

  <T> Provider<T> getProvider(Key<T> key);

  <T> Provider<T> getProvider(Class<T> type);

  <T> T getInstance(Key<T> key);

  <T> T getInstance(Class<T> type);
}
