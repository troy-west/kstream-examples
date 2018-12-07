(defproject troy-west/kstream-examples "0.1.0-SNAPSHOT"

  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/tools.logging "0.4.1"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [com.cognitect/transit-clj "0.8.313"]
                 [org.apache.kafka/kafka-streams-test-utils "2.1.0"]]

  :aot [troy-west.serdes])
