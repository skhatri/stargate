package io.stargate.db.query;

import io.stargate.db.query.builder.Bindable;
import java.util.Optional;
import org.apache.cassandra.stargate.exceptions.InvalidRequestException;
import org.apache.cassandra.stargate.utils.MD5Digest;

/**
 * A CQL query.
 *
 * <p>Implementations of this class make no strong guarantees on the validity of the query they
 * represent; executing them may raise an {@link InvalidRequestException}.
 */
public interface Query<B extends BoundQuery> extends Bindable<B> {

  /** If this is a prepared query, it's prepared ID. */
  Optional<MD5Digest> preparedId();

  /** Creates a prepared copy of this query, using the provided prepared ID. */
  Query<B> withPreparedId(MD5Digest preparedId);

  /**
   * The query string that must be used to prepare this query.
   *
   * <p>Please note that the query string returned by this method may or may not be equal to the one
   * of {@link #toString()} (in particular, the query is allowed to prepare some of values that are
   * not part of {@link #bindMarkers()}, to optimize prepared statement caching for instance).
   */
  String queryStringForPreparation();

  /**
   * A valid CQL query string representation of this query (with bind markers for the values of
   * {@link #bindMarkers()}).
   */
  @Override
  String toString();
}
