package io.stargate.db.query.builder;

import static com.datastax.oss.driver.shaded.guava.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

import io.stargate.db.query.AsyncQueryExecutor;
import io.stargate.db.query.BindMarker;
import io.stargate.db.query.BoundDMLQuery;
import io.stargate.db.query.Condition;
import io.stargate.db.query.ImmutableCondition;
import io.stargate.db.query.ImmutableModification;
import io.stargate.db.query.KeyRange;
import io.stargate.db.query.KeyRange.Bound;
import io.stargate.db.query.ModifiableEntity;
import io.stargate.db.query.Modification;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.PrimaryKey;
import io.stargate.db.query.QueryType;
import io.stargate.db.query.RowsUpdated;
import io.stargate.db.query.RowsUpdated.Ranges;
import io.stargate.db.query.TypedValue;
import io.stargate.db.query.TypedValue.Codec;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.Table;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.cassandra.stargate.utils.MD5Digest;
import org.javatuples.Pair;

abstract class BuiltDML<Q extends AbstractBound<?> & BoundDMLQuery> extends BuiltQuery<Q> {

  private final Table table;
  protected final DMLData data;

  protected BuiltDML(
      QueryType queryType,
      Table table,
      Codec codec,
      AsyncQueryExecutor executor,
      QueryStringBuilder builder,
      List<BuiltCondition> where,
      List<ValueModifier> modifiers,
      List<BuiltCondition> conditions,
      @Nullable Value<Integer> ttlValue,
      @Nullable Value<Long> timestampValue) {
    this(
        queryType,
        table,
        codec,
        null,
        executor,
        builder.externalBindMarkers(),
        new DMLData(
            builder.externalQueryString(),
            builder.internalBindMarkers(),
            builder.internalQueryString(),
            where,
            modifiers,
            conditions,
            ttlValue,
            timestampValue));
  }

  protected BuiltDML(
      QueryType queryType,
      Table table,
      Codec codec,
      @Nullable MD5Digest preparedId,
      AsyncQueryExecutor executor,
      List<BindMarker> externalBindMarkers,
      DMLData data) {
    super(queryType, codec, preparedId, executor, externalBindMarkers);
    this.table = table;
    this.data = data;
  }

  @Override
  public String queryStringForPreparation() {
    return data.internalQueryString;
  }

  public Table table() {
    return table;
  }

  @Override
  protected Q createBoundQuery(List<TypedValue> values) {
    BoundInfo info = new BoundInfo(this, values);
    info.handleWhereClause();
    info.handleRegularAndStaticModifications();
    info.handleTimestamp();
    info.handleTTL();
    info.handleConditions();
    return createBoundQuery(info);
  }

  protected abstract Q createBoundQuery(BoundInfo builder);

  @Override
  public final String toString() {
    return data.externalQueryString;
  }

  // This class only exists to avoid BuiltDML sub-classes to have to pass all those arguments
  // when creating copy of themselves for their withPreparedId() implementation.
  protected static class DMLData {
    private final String externalQueryString;
    private final int internalBindMarkerCount;
    private final String internalQueryString;
    private final List<BuiltCondition> where;
    private final List<ValueModifier> regularAndStaticModifiers;
    private final List<BuiltCondition> conditions;
    private final @Nullable Value<Integer> ttlValue;
    private final @Nullable Value<Long> timestampValue;

    private DMLData(
        String externalQueryString,
        int internalBindMarkerCount,
        String internalQueryString,
        List<BuiltCondition> where,
        List<ValueModifier> regularAndStaticModifiers,
        List<BuiltCondition> ifConditions,
        @Nullable Value<Integer> ttlValue,
        @Nullable Value<Long> timestampValue) {
      this.externalQueryString = externalQueryString;
      this.internalBindMarkerCount = internalBindMarkerCount;
      this.internalQueryString = internalQueryString;
      this.where = where;
      this.regularAndStaticModifiers = regularAndStaticModifiers;
      this.conditions = ifConditions;
      this.ttlValue = ttlValue;
      this.timestampValue = timestampValue;
    }
  }

  protected static class BoundInfo {
    private final BuiltDML<?> dml;
    private final List<TypedValue> boundValues;
    private final TypedValue[] internalBoundValues;
    private RowsUpdated rowsUpdated;
    private final List<Modification> modifications = new ArrayList<>();
    private final List<Condition> conditions = new ArrayList<>();
    private Integer ttl;
    private Long timestamp;

    BoundInfo(BuiltDML<?> dml, List<TypedValue> boundValues) {
      this.dml = dml;
      this.boundValues = boundValues;
      this.internalBoundValues = new TypedValue[dml.data.internalBindMarkerCount];
    }

    List<TypedValue> boundedValues() {
      return boundValues;
    }

    private void handleWhereClause() {
      List<Column> primaryKeys = dml.table.primaryKeyColumns();
      PKCondition[] pkConditions = createPKConditions(primaryKeys);

      rowsUpdated =
          isKeys(pkConditions)
              ? createKeys(primaryKeys, pkConditions)
              : createRange(primaryKeys, pkConditions);
    }

    private boolean isKeys(PKCondition[] pkConditions) {
      PKCondition last = pkConditions[pkConditions.length - 1];
      return last != null && last.isEqOrIn();
    }

    private Ranges createRange(List<Column> primaryKeys, PKCondition[] pkConditions) {
      Pair<List<List<TypedValue>>, Integer> p = populateEqAndIn(primaryKeys, pkConditions);
      int firstNonEq = p.getValue1();
      assert firstNonEq < primaryKeys.size();
      List<List<TypedValue>> pkValues = p.getValue0();

      PKCondition condition = pkConditions[firstNonEq];
      List<KeyRange> ranges = new ArrayList<>(pkValues.size());
      for (List<TypedValue> pkPrefix : pkValues) {
        List<TypedValue> startValues = pkPrefix;
        boolean startInclusive = true;
        List<TypedValue> endValues = pkPrefix;
        boolean endInclusive = true;
        if (condition != null) {
          startValues = new ArrayList<>(pkPrefix);
          startValues.add(condition.values[0]);
          startInclusive = condition.isInclusive[0];
          endValues = new ArrayList<>(pkPrefix);
          endValues.add(condition.values[1]);
          endInclusive = condition.isInclusive[1];
        }
        KeyRange.Bound start = new Bound(startValues, startInclusive);
        KeyRange.Bound end = new Bound(endValues, endInclusive);
        ranges.add(new KeyRange(start, end));
      }
      return new Ranges(ranges);
    }

    private RowsUpdated.Keys createKeys(List<Column> primaryKeys, PKCondition[] pkConditions) {
      Pair<List<List<TypedValue>>, Integer> p = populateEqAndIn(primaryKeys, pkConditions);
      // All keys must have been consumed, or we had a unexpected condition
      checkArgument(
          p.getValue1() == primaryKeys.size(),
          "Invalid condition combinations on primary key columns");
      List<List<TypedValue>> pkValues = p.getValue0();
      List<PrimaryKey> keys = new ArrayList<>(pkValues.size());
      for (List<TypedValue> pkValue : pkValues) {
        keys.add(new PrimaryKey(dml.table, pkValue));
      }
      return new RowsUpdated.Keys(keys);
    }

    private Pair<List<List<TypedValue>>, Integer> populateEqAndIn(
        List<Column> primaryKeys, PKCondition[] pkConditions) {
      List<List<TypedValue>> pkValues = new ArrayList<>();
      pkValues.add(new ArrayList<>());
      for (int i = 0; i < primaryKeys.size(); i++) {
        Column column = primaryKeys.get(i);
        PKCondition condition = pkConditions[i];
        if (condition == null || !condition.isEqOrIn()) {
          return Pair.with(pkValues, i);
        }

        TypedValue value = condition.values[0];
        if (condition.isEq()) {
          // Adds the new value to all keys.
          for (List<TypedValue> pk : pkValues) {
            pk.add(value);
          }
        } else { // It's a IN
          assert value.javaValue() instanceof List; // Things would have failed before otherwise
          List<?> inValues = (List<?>) value.javaValue();
          List<TypedValue> inTypedValues =
              inValues.stream()
                  .map(
                      o ->
                          TypedValue.forJavaValue(
                              dml.valueCodec(), column.name(), column.type(), o))
                  .collect(Collectors.toList());
          // For each existing primary keys, creates #inValues new keys corresponding to the
          // previous key plus the new value.
          // TODO: note that if we're not careful with the generated queries, we can have a
          //  combinatorial explosion here. I could swear C* had limits for this, rejecting queries
          //  that would create too many keys, but I can't find it right now, so maybe not. We
          //  may want to add in any case, but it's unclear what a good number is.
          List<List<TypedValue>> currentValues = pkValues;
          pkValues = new ArrayList<>();
          for (List<TypedValue> currentValue : currentValues) {
            for (TypedValue newValue : inTypedValues) {
              List<TypedValue> newValues = new ArrayList<>(currentValue);
              newValues.add(newValue);
              pkValues.add(newValues);
            }
          }
        }
      }
      return Pair.with(pkValues, primaryKeys.size());
    }

    private PKCondition[] createPKConditions(List<Column> primaryKeys) {
      checkArgument(!primaryKeys.isEmpty(), "Missing WHERE condition on partition key");
      PKCondition[] pkConditions = new PKCondition[primaryKeys.size()];
      for (BuiltCondition condition : dml.data.where) {
        BuiltCondition.LHS lhs = condition.lhs();
        checkArgument(
            lhs.isColumnName(),
            "Invalid condition %s: cannot have condition on the sub-part of a primary key",
            lhs);

        Column column =
            dml.table.existingColumn(((BuiltCondition.LHS.ColumnName) lhs).columnName());
        checkArgument(
            column.isPrimaryKeyComponent(),
            "Invalid WHERE condition for DML on non primary key column %s",
            column.cqlName());

        int idx = dml.table.primaryKeyColumnIndex(column);
        pkConditions[idx] = compute(column, pkConditions[idx], condition);
      }
      return pkConditions;
    }

    private void handleRegularAndStaticModifications() {
      for (ValueModifier modifier : dml.data.regularAndStaticModifiers) {
        Column column = dml.table.existingColumn(modifier.target().columnName());
        ModifiableEntity entity = createEntity(column, modifier.target());
        TypedValue v = handleValue(entity.toString(), entity.type(), modifier.value());
        if (!v.isUnset()) {
          modifications.add(
              ImmutableModification.builder()
                  .entity(entity)
                  .operation(modifier.operation())
                  .value(v)
                  .build());
        }
      }
    }

    private ModifiableEntity createEntity(Column column, ValueModifier.Target target) {
      if (target.fieldName() != null) {
        return ModifiableEntity.udtType(column, target.fieldName());
      } else if (target.mapKey() != null) {
        ColumnType type = column.type();
        checkArgument(type != null, "Provided column does not have its type set");
        checkArgument(
            type.isMap(),
            "Cannot access elements of column %s of type %s in %s, it is not a map",
            column.cqlName(),
            type,
            dml.table.cqlQualifiedName());
        ColumnType keyType = type.parameters().get(0);
        Value<?> key = target.mapKey();
        TypedValue keyValue =
            dml.convertValue(key, "element of " + column.cqlName(), keyType, boundValues);
        checkArgument(
            !keyValue.isUnset(),
            "Cannot use UNSET for map column %s key in table %s",
            column.cqlName(),
            dml.table.cqlQualifiedName());
        return ModifiableEntity.mapValue(column, keyValue);
      } else {
        return ModifiableEntity.of(column);
      }
    }

    private TypedValue handleValue(Column column, Value<?> value) {
      return handleValue(column.cqlName(), column.type(), value);
    }

    private TypedValue handleValue(String entityName, ColumnType type, Value<?> value) {
      TypedValue v = dml.convertValue(value, entityName, type, boundValues);
      int internalIndex = value.internalIndex();
      if (internalIndex >= 0) {
        internalBoundValues[internalIndex] = v;
      }
      return v;
    }

    private void handleTTL() {
      if (dml.data.ttlValue == null) {
        return;
      }
      TypedValue v = handleValue(Column.TTL, dml.data.ttlValue);
      if (!v.isUnset()) {
        ttl = (Integer) v.javaValue();
        if (ttl == null) {
          throw new IllegalArgumentException("Cannot pass null as bound value for the TTL");
        }
      }
    }

    private void handleTimestamp() {
      if (dml.data.timestampValue == null) {
        return;
      }
      TypedValue v = handleValue(Column.TIMESTAMP, dml.data.timestampValue);
      if (!v.isUnset()) {
        timestamp = (Long) v.javaValue();
        if (timestamp == null) {
          throw new IllegalArgumentException("Cannot pass null as bound value for the TIMESTAMP");
        }
      }
    }

    private void handleConditions() {
      for (BuiltCondition bc : dml.data.conditions) {
        Condition.LHS lhs = createLHS(bc.lhs());
        TypedValue v = handleValue(lhs.toString(), lhs.valueType(), bc.value());
        if (!lhs.isUnset() && !v.isUnset()) {
          conditions.add(
              ImmutableCondition.builder().lhs(lhs).predicate(bc.predicate()).value(v).build());
        }
      }
    }

    private Condition.LHS createLHS(BuiltCondition.LHS lhs) {
      if (lhs.isColumnName()) {
        String name = ((BuiltCondition.LHS.ColumnName) lhs).columnName();
        return Condition.LHS.column(dml.table.existingColumn(name));
      } else if (lhs.isMapAccess()) {
        BuiltCondition.LHS.MapElement m = ((BuiltCondition.LHS.MapElement) lhs);
        Column column = dml.table.existingColumn(m.columnName());
        TypedValue v = handleValue(m.toString(), column.type().parameters().get(0), m.keyValue());
        return Condition.LHS.mapAccess(column, v);
      } else {
        throw new UnsupportedOperationException();
      }
    }

    List<TypedValue> internalBoundValues() {
      return Arrays.asList(internalBoundValues);
    }

    RowsUpdated rowsUpdated() {
      return rowsUpdated;
    }

    List<Modification> modifications() {
      return modifications;
    }

    List<Condition> conditions() {
      return conditions;
    }

    OptionalInt ttl() {
      return ttl == null ? OptionalInt.empty() : OptionalInt.of(ttl);
    }

    OptionalLong timestamp() {
      return timestamp == null ? OptionalLong.empty() : OptionalLong.of(timestamp);
    }

    private boolean isEqOrIn(BuiltCondition condition) {
      return condition.predicate() == Predicate.EQ || condition.predicate() == Predicate.IN;
    }

    private int rangeIdx(BuiltCondition condition) {
      switch (condition.predicate()) {
        case GT:
        case GTE:
          return 0;
        case LT:
        case LTE:
          return 1;
        default:
          throw new IllegalArgumentException(
              "Invalidate condition on primary key column: " + condition);
      }
    }

    private boolean isInclusive(BuiltCondition condition) {
      switch (condition.predicate()) {
        case GTE:
        case LTE:
          return true;
        case GT:
        case LT:
          return false;
        default:
          // This should be called after rangeIdx, which already rejected those.
          throw new AssertionError();
      }
    }

    private PKCondition compute(Column column, PKCondition existing, BuiltCondition condition) {

      TypedValue v;
      if (condition.predicate() == Predicate.IN) {
        v =
            handleValue(
                format("in(%s)", column.name()), Type.List.of(column.type()), condition.value());
      } else {
        v = handleValue(column, condition.value());
      }
      checkArgument(
          v.bytes() != null,
          "Cannot use a null value for primary key column %s in table %s",
          column.cqlName(),
          dml.table.cqlQualifiedName());

      if (v.isUnset()) {
        return existing;
      }

      if (isEqOrIn(condition)) {
        if (existing != null) {
          throw new IllegalArgumentException(
              format("Incompatible conditions %s and %s", existing.firstCondition, condition));
        }

        PKCondition pkCondition = new PKCondition(condition);
        pkCondition.values[0] = v;
        return pkCondition;
      }

      checkArgument(
          !column.isPartitionKey(),
          "Invalid condition %s for partition key %s",
          condition,
          column.cqlName());

      if (existing == null) {
        existing = new PKCondition(condition);
      } else {
        checkArgument(
            !existing.isEqOrIn(),
            "Incompatible conditions %s and %s",
            existing.firstCondition,
            condition);
      }
      int idx = rangeIdx(condition);
      checkArgument(
          existing.values[idx] == null,
          "Incompatible conditions %s and %s",
          existing.firstCondition,
          condition);
      existing.values[idx] = v;
      existing.isInclusive[idx] = isInclusive(condition);
      return existing;
    }

    private class PKCondition {
      private final BuiltCondition firstCondition;
      // for Eq/IN, only 0 is set, for ranges, 0 = open, 1 = close
      private final TypedValue[] values = new TypedValue[2];
      // Only set for ranges, 0 = open, 1 = close
      private final boolean[] isInclusive = new boolean[2];

      private PKCondition(BuiltCondition firstCondition) {
        this.firstCondition = firstCondition;
      }

      private boolean isEq() {
        return firstCondition.predicate() == Predicate.EQ;
      }

      private boolean isEqOrIn() {
        return BoundInfo.this.isEqOrIn(firstCondition);
      }
    }
  }
}
