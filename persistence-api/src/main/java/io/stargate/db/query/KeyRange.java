package io.stargate.db.query;

import java.util.List;
import java.util.Objects;

/** A range of primary keys. */
public class KeyRange {
  private final Bound start;
  private final Bound end;

  public KeyRange(Bound start, Bound end) {
    this.start = start;
    this.end = end;
  }

  public Bound start() {
    return start;
  }

  public Bound end() {
    return end;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KeyRange keyRange = (KeyRange) o;
    return start.equals(keyRange.start) && end.equals(keyRange.end);
  }

  @Override
  public int hashCode() {
    return Objects.hash(start, end);
  }

  public static class Bound {
    private final List<TypedValue> values;
    private final boolean isInclusive;

    public Bound(List<TypedValue> values, boolean isInclusive) {
      this.values = values;
      this.isInclusive = isInclusive;
    }

    public List<TypedValue> values() {
      return values;
    }

    public boolean isInclusive() {
      return isInclusive;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Bound bound = (Bound) o;
      return isInclusive == bound.isInclusive && values.equals(bound.values);
    }

    @Override
    public int hashCode() {
      return Objects.hash(values, isInclusive);
    }
  }
}
