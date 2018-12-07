(ns troy-west.serdes
  (:require [cognitect.transit :as transit])
  (:import (org.apache.kafka.common.serialization Deserializer Serializer Serde)
           (java.io ByteArrayInputStream ByteArrayOutputStream)))

(defn do-serialize
  [format data]
  (when data
    (let [stream (ByteArrayOutputStream.)]
      (transit/write (transit/writer stream format nil) data)
      (.toByteArray stream))))

(defn do-deserialize
  [format data]
  (when data
    (transit/read (transit/reader (ByteArrayInputStream. data) format nil))))

(deftype JsonSerializer []
  Serializer
  (configure [_ _ _])
  (serialize [_ _ data] (do-serialize :json data))
  (close [_]))

(deftype JsonDeserializer []
  Deserializer
  (configure [_ _ _])
  (deserialize [_ _ data] (do-deserialize :json data))
  (close [_]))

(deftype JsonSerde []
  Serde
  (configure [_ _ _])
  (close [_])
  (serializer [_] (JsonSerializer.))
  (deserializer [_] (JsonDeserializer.)))

(deftype JsonVerboseSerializer []
  Serializer
  (configure [_ _ _])
  (serialize [_ _ data] (do-serialize :json-verbose data))
  (close [_]))

(deftype JsonVerboseDeserializer []
  Deserializer
  (configure [_ _ _])
  (deserialize [_ _ data] (do-deserialize :json-verbose data))
  (close [_]))

(deftype JsonVerboseSerde []
  Serde
  (configure [_ _ _])
  (close [_])
  (serializer [_] (JsonSerializer.))
  (deserializer [_] (JsonDeserializer.)))

(deftype MsgpackSerializer []
  Serializer
  (configure [_ _ _])
  (serialize [_ _ data] (do-serialize :msgpack data))
  (close [_]))

(deftype MsgpackDeserializer []
  Deserializer
  (configure [_ _ _])
  (deserialize [_ _ data] (do-deserialize :msgpack data))
  (close [_]))

(deftype MsgpackSerde []
  Serde
  (configure [_ _ _])
  (close [_])
  (serializer [_] (MsgpackSerializer.))
  (deserializer [_] (MsgpackDeserializer.)))

(defn serializer
  [format]
  (case format
    :json (->JsonSerializer)
    :json-verbose (->JsonVerboseSerializer)
    :msgpack (->MsgpackSerializer)))

(defn deserializer
  [format]
  (case format
    :json (->JsonDeserializer)
    :json-verbose (->JsonVerboseDeserializer)
    :msgpack (->MsgpackDeserializer)))

(defn serde
  [format]
  (case format
    :json (->JsonSerde)
    :json-verbose (->JsonVerboseSerde)
    :msgpack (->MsgpackSerde)))