package io.stargate.db.query;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.util.List;
import java.util.Objects;

/**
 * Specify which rows are updated by a given DML query.
 *
 * <p>There is effectively two implementation of this class, corresponding to the 2 ways rows can be
 * updated in CQL: either a set of specific rows are impacted ()
 *
 * <ul>
 *   <li>
 * </ul>
 */
public abstract class RowsUpdated {
  private RowsUpdated() {}

  public abstract boolean isRanges();

  public boolean isKeys() {
    return !isRanges();
  }

  public Keys asKeys() {
    Preconditions.checkState(isKeys());
    return (Keys) this;
  }

  public Ranges asRanges() {
    Preconditions.checkState(isRanges());
    return (Ranges) this;
  }

  public static class Keys extends RowsUpdated {
    private final List<PrimaryKey> primaryKeys;

    public Keys(List<PrimaryKey> primaryKeys) {
      this.primaryKeys = primaryKeys;
    }

    public List<PrimaryKey> primaryKeys() {
      return primaryKeys;
    }

    @Override
    public boolean isRanges() {
      return false;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Keys keys = (Keys) o;
      return primaryKeys.equals(keys.primaryKeys);
    }

    @Override
    public int hashCode() {
      return Objects.hash(primaryKeys);
    }

    @Override
    public String toString() {
      return primaryKeys.toString();
    }
  }

  public static class Ranges extends RowsUpdated {
    private final List<KeyRange> keyRanges;

    public Ranges(List<KeyRange> keyRanges) {
      this.keyRanges = keyRanges;
    }

    public List<KeyRange> ranges() {
      return keyRanges;
    }

    @Override
    public boolean isRanges() {
      return true;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Ranges ranges = (Ranges) o;
      return keyRanges.equals(ranges.keyRanges);
    }

    @Override
    public int hashCode() {
      return Objects.hash(keyRanges);
    }
  }
}
