package io.stargate.db.query.builder;

import io.stargate.db.query.BindMarker;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.TypedValue;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public interface Bindable<B extends BoundQuery> {

  TypedValue.Codec valueCodec();

  /** The (potentially empty) list of bind markers that remains to be bound in this query. */
  List<BindMarker> bindMarkers();

  B bindValues(List<TypedValue> values);

  default B bind(Object... values) {
    return bind(Arrays.asList(values));
  }

  default B bind(List<Object> values) {
    return bindValues(TypedValue.forJavaValues(valueCodec(), bindMarkers(), values));
  }

  default B bindBuffers(ByteBuffer... values) {
    return bindBuffers(Arrays.asList(values));
  }

  default B bindBuffers(List<ByteBuffer> values) {
    return bindValues(TypedValue.forBytesValues(valueCodec(), bindMarkers(), values));
  }
}
