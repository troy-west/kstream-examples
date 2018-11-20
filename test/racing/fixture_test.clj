(ns racing.fixture-test
  (:require [clojure.test :refer :all]
            [serdes :as serdes])
  (:import (java.util Date Properties)
           (org.apache.kafka.streams TopologyTestDriver StreamsBuilder)
           (org.apache.kafka.streams.kstream Predicate TimeWindows Windowed KeyValueMapper Reducer Initializer Aggregator)
           (org.apache.kafka.streams.test ConsumerRecordFactory)
           (org.apache.kafka.clients.producer ProducerRecord)
           (org.apache.kafka.common.serialization StringDeserializer StringSerializer)))

(defn read-output
  [driver topic]
  (when-let [record (.readOutput ^TopologyTestDriver
                                 driver
                                 topic
                                 (StringDeserializer.)
                                 (serdes/->JsonDeserializer))]
    (.value ^ProducerRecord record)))

;; We create some boilerplate code for testing Kafka Streams
(let [factory (ConsumerRecordFactory. "fixtures"
                                      (StringSerializer.)
                                      (serdes/->JsonSerializer)
                                      (.getTime (Date.))
                                      6000)
      config  (let [props (Properties.)]
                (.putAll props {"application.id"      "test-racing-fixtures"
                                "bootstrap.servers"   "dummy:1234"
                                "default.key.serde"   "org.apache.kafka.common.serialization.Serdes$StringSerde"
                                "default.value.serde" "serdes.JsonSerde"})
                props)]

  (deftest filter-closed

    ;; We create a builder and use it to define a topology, in this case filtering closed fixtures to a new topic.
    (let [builder (StreamsBuilder.)]

      (-> (.stream builder "fixtures")
          (.filter (reify Predicate
                     (test [_ _ event]
                       (true? (= "closed" (:state event))))))
          (.to "closed-fixtures"))

      (with-open [driver (TopologyTestDriver. (.build builder) config)]

        ;; Test that by sending the input topic multiple events
        (let [event-1 (.create factory "fixtures" "race-1" {:id "race-1" :state "open"})
              event-2 (.create factory "fixtures" "race-1" {:id "race-1" :state "open"})
              event-3 (.create factory "fixtures" "race-2" {:id "race-2" :state "open"})
              event-4 (.create factory "fixtures" "race-1" {:id "race-1" :state "open"})
              event-5 (.create factory "fixtures" "race-1" {:id "race-1" :state "closed"})
              event-6 (.create factory "fixtures" "race-3" {:id "race-3" :state "open"})]

          ;; and checking that the closed-fixtures topic contains only the closed event
          (is (= {:id "race-1" :state "closed"}
                 (do (.pipeInput driver event-1)
                     (.pipeInput driver event-2)
                     (.pipeInput driver event-3)
                     (.pipeInput driver event-4)
                     (.pipeInput driver event-5)
                     (.pipeInput driver event-6)
                     (read-output ^TopologyTestDriver driver "closed-fixtures"))))))))

  (deftest hopping-time-windows

    ;; In this test we'll start to play with hopping-time-windows to explore how they work
    (let [builder (StreamsBuilder.)]

      (-> (.stream builder "fixtures")
          (.groupByKey)
          (.windowedBy (TimeWindows/of 20000))
          (.aggregate (reify Initializer
                        (apply [_] []))
                      (reify Aggregator
                        (apply [_ _ event agg]
                          (conj agg event))))
          (.toStream (reify KeyValueMapper
                       (apply [_ k _]
                         (.key ^Windowed k))))
          (.to "closed-fixtures"))

      (with-open [driver (TopologyTestDriver. (.build builder) config)]

        ;; Test that by sending the input topic multiple events
        (let [event-1 (.create factory "fixtures" "race-1" {:id "race-1" :state "open"})
              event-2 (.create factory "fixtures" "race-1" {:id "race-1" :state "open" :attr "x"})
              event-3 (.create factory "fixtures" "race-1" {:id "race-1" :state "open" :attr "y"})
              event-4 (.create factory "fixtures" "race-1" {:id "race-1" :state "open" :attr "z"})]

          ;; and checking that the closed-fixtures topic contains only the closed event
          (is (= [{:id "race-1" :state "open"}]
                 (do (.pipeInput driver event-1)
                     (.pipeInput driver event-2)
                     (.pipeInput driver event-3)
                     (.pipeInput driver event-4)
                     [(read-output ^TopologyTestDriver driver "closed-fixtures")
                      (read-output ^TopologyTestDriver driver "closed-fixtures")
                      (read-output ^TopologyTestDriver driver "closed-fixtures")
                      (read-output ^TopologyTestDriver driver "closed-fixtures")
                      ]))))))))