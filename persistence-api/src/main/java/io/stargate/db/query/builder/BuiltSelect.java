package io.stargate.db.query.builder;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import io.stargate.db.query.AsyncQueryExecutor;
import io.stargate.db.query.BindMarker;
import io.stargate.db.query.BoundSelect;
import io.stargate.db.query.Query;
import io.stargate.db.query.QueryType;
import io.stargate.db.query.TypedValue;
import io.stargate.db.query.TypedValue.Codec;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.cassandra.stargate.utils.MD5Digest;

public class BuiltSelect extends BuiltQuery<BuiltSelect.Bound> {
  private final Table table;
  private final String externalQueryString;
  private final String internalQueryString;
  private final Set<Column> selectedColumns;
  private final List<Value<?>> internalValues;
  private final List<BindMarker> internalBindMarkers;

  protected BuiltSelect(
      Table table,
      Codec codec,
      AsyncQueryExecutor executor,
      QueryStringBuilder builder,
      Set<Column> selectedColumns,
      List<Value<?>> internalValues,
      List<BindMarker> internalBindMarkers) {
    this(
        table,
        codec,
        null,
        executor,
        builder.externalQueryString(),
        builder.externalBindMarkers(),
        builder.internalQueryString(),
        selectedColumns,
        internalValues,
        internalBindMarkers);
    Preconditions.checkArgument(
        builder.internalBindMarkers() == internalValues.size(),
        "Provided %s values, but the builder has seen %s values",
        internalValues.size(),
        builder.internalBindMarkers());
  }

  private BuiltSelect(
      Table table,
      Codec codec,
      @Nullable MD5Digest preparedId,
      AsyncQueryExecutor executor,
      String externalQueryString,
      List<BindMarker> unboundMarkers,
      String internalQueryString,
      Set<Column> selectedColumns,
      List<Value<?>> internalValues,
      List<BindMarker> internalBindMarkers) {
    super(QueryType.SELECT, codec, preparedId, executor, unboundMarkers);
    this.table = table;
    this.internalQueryString = internalQueryString;
    this.externalQueryString = externalQueryString;
    this.selectedColumns = selectedColumns;
    this.internalValues = internalValues;
    this.internalBindMarkers = internalBindMarkers;
  }

  public Table table() {
    return table;
  }

  public Set<Column> selectedColumns() {
    return selectedColumns;
  }

  @Override
  public String queryStringForPreparation() {
    return internalQueryString;
  }

  @Override
  protected BuiltSelect.Bound createBoundQuery(List<TypedValue> values) {
    TypedValue[] internalBoundValues = new TypedValue[internalValues.size()];
    for (int i = 0; i < internalValues.size(); i++) {
      Value<?> internalValue = internalValues.get(i);
      BindMarker internalMarker = internalBindMarkers.get(i);
      TypedValue v =
          convertValue(internalValue, internalMarker.receiver(), internalMarker.type(), values);
      int internalIndex = internalValue.internalIndex();
      Preconditions.checkState(internalIndex >= 0);
      internalBoundValues[internalIndex] = v;
    }
    return new Bound(this, values, Arrays.asList(internalBoundValues));
  }

  @Override
  public Query<Bound> withPreparedId(MD5Digest preparedId) {
    return new BuiltSelect(
        table(),
        valueCodec(),
        preparedId,
        executor(),
        externalQueryString,
        bindMarkers(),
        internalQueryString,
        selectedColumns,
        internalValues,
        internalBindMarkers);
  }

  @Override
  public final String toString() {
    return externalQueryString;
  }

  public static class Bound extends AbstractBound<BuiltSelect> implements BoundSelect {
    private Bound(BuiltSelect builtQuery, List<TypedValue> boundedValues, List<TypedValue> values) {
      super(builtQuery, boundedValues, values);
    }

    @Override
    public Table table() {
      return bounded().query().table();
    }

    @Override
    public Set<Column> selectedColumns() {
      return bounded().query().selectedColumns();
    }

    private String addColumnsToQueryString(
        Set<Column> toAdd, String queryString, boolean isStarSelect) {
      StringBuilder sb = new StringBuilder();
      if (isStarSelect) {
        int idx = queryString.indexOf('*');
        Preconditions.checkState(idx > 0, "Should have found '*' in %s", queryString);
        sb.append(queryString, 0, idx);
        sb.append(toAdd.stream().map(Column::cqlName).collect(Collectors.joining(", ")));
        // +1 to skip the '*'
        sb.append(queryString, idx + 1, queryString.length());
      } else {
        int idx = queryString.indexOf(" FROM ");
        Preconditions.checkState(idx > 0, "Should have found 'FROM' in %s", queryString);
        sb.append(queryString, 0, idx);
        for (Column c : toAdd) {
          sb.append(", ").append(c.cqlName());
        }
        sb.append(queryString, idx, queryString.length());
      }
      return sb.toString();
    }

    @Override
    public BoundSelect withAddedSelectedColumns(Set<Column> columns) {
      Set<Column> toAdd =
          columns.stream().filter(c -> !selectedColumns().contains(c)).collect(Collectors.toSet());
      if (toAdd.isEmpty()) {
        return this;
      }

      Set<Column> newSelectedColumns = new HashSet<>(selectedColumns());
      newSelectedColumns.addAll(toAdd);

      BuiltSelect oldBuilt = bounded().query();
      BuiltSelect newBuilt =
          new BuiltSelect(
              table(),
              oldBuilt.valueCodec(),
              null,
              oldBuilt.executor(),
              addColumnsToQueryString(toAdd, oldBuilt.externalQueryString, isStarSelect()),
              oldBuilt.bindMarkers(),
              addColumnsToQueryString(toAdd, oldBuilt.internalQueryString, isStarSelect()),
              newSelectedColumns,
              oldBuilt.internalValues,
              oldBuilt.internalBindMarkers);
      return new Bound(newBuilt, bounded().values(), values());
    }
  }
}
