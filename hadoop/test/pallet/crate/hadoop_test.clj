(ns pallet.crate.hadoop-test
  (:require
   [pallet.crate.hadoop :as hadoop]
   [pallet.build-actions :as ba])
  (:use clojure.test))

;; (deftest hadoop-test
;;   (is                          ; just check for compile errors for now
;;    (ba/build-actions
;;     []
;;     (hadoop/name-node "data-dir")
;;     (hadoop/secondary-name-node)
;;     (hadoop/job-tracker)
;;     (hadoop/data-node)
;;     (hadoop/task-tracker))))
