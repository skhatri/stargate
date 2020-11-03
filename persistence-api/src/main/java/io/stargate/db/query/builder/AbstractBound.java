package io.stargate.db.query.builder;

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import io.stargate.db.Parameters;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.query.AsyncQueryExecutor;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.QueryType;
import io.stargate.db.query.TypedValue;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

// In theory, we should access this outside of this package. However, some test cast a BoundQuery
// into this class to reach into the underlying BuiltQuery, and this out of convenience. This could
// be made package private by changing those tests, but it's probably not a big deal.
@VisibleForTesting
public class AbstractBound<Q extends BuiltQuery<?>> implements BoundQuery {
  private final Bounded<Q> bounded;
  private final List<TypedValue> values;

  AbstractBound(Q builtQuery, List<TypedValue> boundedValues, List<TypedValue> values) {
    this.bounded = new Bounded<>(builtQuery, boundedValues);
    this.values = values;
  }

  @Override
  public QueryType type() {
    return bounded.query().type();
  }

  @Override
  public Bounded<Q> bounded() {
    return bounded;
  }

  @Override
  public List<TypedValue> values() {
    return values;
  }

  private AsyncQueryExecutor executor() {
    Preconditions.checkState(
        bounded.query().executor() != null, "Cannot execute query: it has no attached executor");
    return bounded.query().executor();
  }

  /**
   * Executes this query using the underlying executor the query was built with.
   *
   * <p>See the {@link AsyncQueryExecutor#execute(BoundQuery)}.
   */
  public CompletableFuture<ResultSet> execute() {
    return executor().execute(this);
  }

  /**
   * Executes this query using the underlying executor the query was built with.
   *
   * <p>See the {@link AsyncQueryExecutor#execute(BoundQuery, ConsistencyLevel)}.
   */
  public CompletableFuture<ResultSet> execute(ConsistencyLevel consistencyLevel) {
    return executor().execute(this, consistencyLevel);
  }

  /**
   * Executes this query using the underlying executor the query was built with.
   *
   * <p>See the {@link AsyncQueryExecutor#execute(BoundQuery, UnaryOperator)}.
   */
  public CompletableFuture<ResultSet> execute(UnaryOperator<Parameters> parametersModifier) {
    return executor().execute(this, parametersModifier);
  }
}
