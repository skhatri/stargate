package io.stargate.db.query;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Table;
import java.util.List;
import java.util.Objects;

/**
 * A row primary key.
 *
 * <p>A primary key object is essentially a list of {@link TypedValue}, but it guarantees that the
 * number of elements in that list is equals to the number of primary keys for the table this is a
 * primary key of, and that the order of values corresponds to those keys as well.
 */
public class PrimaryKey {
  private final Table table;
  private final List<TypedValue> values;

  public PrimaryKey(Table table, List<TypedValue> values) {
    this.table = table;
    this.values = values;

    List<Column> pkColumns = table.primaryKeyColumns();
    Preconditions.checkArgument(
        pkColumns.size() == values.size(),
        "Expected %s primary key values, but only %s provided",
        pkColumns.size(),
        values.size());

    for (int i = 0; i < pkColumns.size(); i++) {
      ColumnType expectedType = pkColumns.get(i).type();
      assert expectedType != null;
      ColumnType actualType = values.get(i).type();
      Preconditions.checkArgument(
          expectedType.equals(actualType),
          "Invalid type for value %s: expecting %s but got %s",
          i,
          expectedType,
          actualType);
    }
  }

  public int size() {
    return values.size();
  }

  public TypedValue get(int i) {
    return values.get(i);
  }

  public TypedValue get(String name) {
    return get(table.existingColumn(name));
  }

  public TypedValue get(Column column) {
    return values.get(table.primaryKeyColumnIndex(column));
  }

  public List<TypedValue> allValues() {
    return values;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PrimaryKey that = (PrimaryKey) o;
    return table.keyspace().equals(that.table.keyspace())
        && table.name().equals(that.table.name())
        && values.equals(that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table.keyspace(), table.name(), values);
  }

  @Override
  public String toString() {
    return values.toString();
  }
}
