(ns pallet.crate.hadoop-test
  (:require
   [pallet.crate.hadoop :as hadoop]
   [pallet.test-utils :as test-utils])
  (:use
   clojure.test))

(deftest hadoop-test
  (is ; just check for compile errors for now
   (test-utils/build-resources
    []
    (hadoop/name-node "data-dir")
    (hadoop/secondary-name-node)
    (hadoop/job-tracker)
    (hadoop/data-node)
    (hadoop/task-tracker))))
