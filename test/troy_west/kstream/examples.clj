(ns troy-west.kstream.examples
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [troy-west.serdes :as serdes])
  (:import (java.util Date Properties)
           (org.apache.kafka.streams TopologyTestDriver StreamsBuilder)
           (org.apache.kafka.streams.kstream Predicate TimeWindows Windowed KeyValueMapper Initializer Aggregator Materialized ValueMapper)
           (org.apache.kafka.streams.test ConsumerRecordFactory)
           (org.apache.kafka.clients.producer ProducerRecord)
           (org.apache.kafka.common.serialization StringDeserializer StringSerializer Serializer)))

;; Some examples of testing Kafka Streams Topologies

;; Basic Rule: Don't build a DSL on top of a perfectly good one, use the Java API.

;; Test config is static across all our tests
(def config (let [props (Properties.)]
              (.putAll props {"application.id"      "test-examples"
                              "bootstrap.servers"   "dummy:1234"
                              "default.key.serde"   "org.apache.kafka.common.serialization.Serdes$StringSerde"
                              "default.value.serde" "troy_west.serdes.JsonSerde"})
              props))

(defn read-output
  [driver topic]
  (when-let [record (.readOutput ^TopologyTestDriver
                                 driver
                                 topic
                                 (StringDeserializer.)
                                 (serdes/->JsonDeserializer))]
    (.value ^ProducerRecord record)))

(deftest filter-closed

  (let [start-time (.getTime (Date.))                       ;; the test start time
        factory    (ConsumerRecordFactory. "events"         ;; A factory that auto-advances its internal time
                                           (StringSerializer.)
                                           (serdes/->JsonSerializer)
                                           start-time
                                           6000)

        ;; We create a builder and use it to define a topology.
        builder    (StreamsBuilder.)]

    ;; In this case filtering closed events to a new topic.
    (-> (.stream builder "events")
        (.filter (reify Predicate
                   (test [_ _ event]
                     (true? (= "closed" (:state event))))))
        (.to "closed-events"))

    (with-open [driver (TopologyTestDriver. (.build builder) config)]

      ;; Test by sending the input topic multiple events
      (.pipeInput driver (.create factory "events" "race-1" {:id "race-1" :state "open"}))
      (.pipeInput driver (.create factory "events" "race-1" {:id "race-1" :state "open"}))
      (.pipeInput driver (.create factory "events" "race-2" {:id "race-2" :state "open"}))
      (.pipeInput driver (.create factory "events" "race-1" {:id "race-1" :state "open"}))
      (.pipeInput driver (.create factory "events" "race-1" {:id "race-1" :state "closed"}))
      (.pipeInput driver (.create factory "events" "race-3" {:id "race-3" :state "open"}))

      ;; and checking that the closed-events topic contains only the closed event
      (is (= {:id "race-1" :state "closed"}
             (read-output ^TopologyTestDriver driver "closed-events"))))))

(deftest mult-stream

  (let [start-time   (.getTime (Date.))                     ;; the test start time
        factory      (ConsumerRecordFactory. "events"       ;; A factory that auto-advances its internal time
                                             (StringSerializer.)
                                             (serdes/->JsonSerializer)
                                             start-time
                                             6000)
        builder      (StreamsBuilder.)
        input-stream (.stream builder "events")]

    ;; It occurs often that clients don't immediately realise that elements of the DSL can have their functions
    ;; called repeatedly in order to 'branch' the topology graph.

    ;; In this case we take an initial topic, transform it two ways, and write it to two output topics
    (-> input-stream
        (.mapValues (reify ValueMapper
                      (apply [_ event]
                        (update event :id str/upper-case))))
        (.to "upper-events"))

    (-> input-stream
        (.mapValues (reify ValueMapper
                      (apply [_ event]
                        (update event :id str/lower-case))))
        (.to "lower-events"))

    (with-open [driver (TopologyTestDriver. (.build builder) config)]

      ;; Send a single event to the input stream
      (.pipeInput driver (.create factory "events" "Race-1" {:id "Race-1" :state "open"}))

      ;; and check that each output topic has the correct form
      (is (= {:id "RACE-1" :state "open"}
             (read-output ^TopologyTestDriver driver "upper-events")))
      (is (= {:id "race-1" :state "open"}
             (read-output ^TopologyTestDriver driver "lower-events"))))))

(deftest tumbling-aggregation-with-specific-time-advancement

  ;; In this test we'll start to play with hopping-time-windows to explore how they work
  (let [start-time (.getTime (Date.))                       ;; the test start time
        factory    (ConsumerRecordFactory. "events"         ;; A factory that *does not* auto advance time
                                           ^Serializer (StringSerializer.)
                                           ^Serializer (serdes/->JsonSerializer)
                                           start-time)
        builder    (StreamsBuilder.)]

    (-> (.stream builder "events")
        (.groupByKey)
        (.windowedBy (-> (TimeWindows/of 20000)
                         (.until 60000)))
        (.aggregate (reify Initializer
                      (apply [_] []))
                    (reify Aggregator
                      (apply [_ _ event agg]
                        (conj agg event)))
                    (Materialized/as "current-events"))
        (.toStream (reify KeyValueMapper
                     (apply [_ k _]
                       (.key ^Windowed k))))
        (.to "processed-events"))

    (with-open [driver (TopologyTestDriver. (.build builder) config start-time)]

      ;; send event one
      (.pipeInput driver (.create factory "events" "race-1" {:id "race-1" :state "open"}))

      ;; increment time and send event two
      (.advanceWallClockTime driver 100)
      (.advanceTimeMs factory 100)
      (.pipeInput driver (.create factory "events" "race-1" {:id "race-1" :state "open" :x 1}))

      ;;increment to a new tumbling window / aggregation and send event three
      (.advanceWallClockTime driver 20000)
      (.advanceTimeMs factory 20000)
      (.pipeInput driver (.create factory "events" "race-1" {:id "race-1" :state "open" :x 2}))

      ;; demonstrate the tumbling window output
      (is (= [[{:id "race-1" :state "open"}]

              [{:id "race-1" :state "open"}
               {:id "race-1" :state "open" :x 1}]

              [{:id "race-1" :state "open" :x 2}]]
             [(read-output ^TopologyTestDriver driver "processed-events")
              (read-output ^TopologyTestDriver driver "processed-events")
              (read-output ^TopologyTestDriver driver "processed-events")]))

      ;; read events back directly from the k-table
      (is (= [[{:id    "race-1"
                :state "open"}
               {:id    "race-1"
                :state "open"
                :x     1}]
              [{:id    "race-1"
                :state "open"
                :x     2}]]
             (map #(.value %1)
                  (iterator-seq
                   (.fetchAll (.getWindowStore driver "current-events")
                              (- start-time 20000)
                              (+ start-time 20000)))))))))