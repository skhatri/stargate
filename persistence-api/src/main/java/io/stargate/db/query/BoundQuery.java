package io.stargate.db.query;

import java.util.List;
import java.util.Objects;

public interface BoundQuery {
  /** The type of query this is. */
  QueryType type();

  /** The query and values used to obtain this bounded query. */
  Bounded<?> bounded();

  /**
   * A CQL query string representation of this query, with bind markers for the values of {@link
   * #values()}.
   */
  default String queryString() {
    return bounded().query().queryStringForPreparation();
  }

  /**
   * The values of this bound query, corresponding to the bind markers for {@link #queryString()}.
   *
   * <p>Please note that those values may or may not be equals to {@code bounded().query().values()}
   * because, as specified in {@link Query#queryStringForPreparation()}, a {@link Query} is allowed
   * to include some additional values (that the ones in {@link Query#bindMarkers()}) in the bound
   * queries it produces.
   */
  List<TypedValue> values();

  class Bounded<Q extends Query<?>> {
    private final Q boundedQuery;
    private final List<TypedValue> boundedValues;

    public Bounded(Q boundedQuery, List<TypedValue> boundedValues) {
      this.boundedQuery = boundedQuery;
      this.boundedValues = boundedValues;
    }

    public Q query() {
      return boundedQuery;
    }

    public List<TypedValue> values() {
      return boundedValues;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Bounded<?> bounded = (Bounded<?>) o;
      return boundedQuery.equals(bounded.boundedQuery)
          && boundedValues.equals(bounded.boundedValues);
    }

    @Override
    public int hashCode() {
      return Objects.hash(boundedQuery, boundedValues);
    }

    @Override
    public String toString() {
      return String.format("%s with values=%s", boundedQuery, boundedValues);
    }
  }
}
