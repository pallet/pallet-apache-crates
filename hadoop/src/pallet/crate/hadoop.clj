; -*- Mode: Clojure; indent-tabs-mode: nil -*-

; Licensed to the Apache Software Foundation (ASF) under one
; or more contributor license agreements.  See the NOTICE file
; distributed with this work for additional information
; regarding copyright ownership.  The ASF licenses this file
; to you under the Apache License, Version 2.0 (the
; "License"); you may not use this file except in compliance
; with the License.  You may obtain a copy of the License at
;     http://www.apache.org/licenses/LICENSE-2.0
; Unless required by applicable law or agreed to in writing, software
; distributed under the License is distributed on an "AS IS" BASIS,
; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
; See the License for the specific language governing permissions and
; limitations under the License.

(ns pallet.crate.hadoop
  "Install and configure hadoop.
   Incomplete - not yet ready for general use."
  (:require
   [pallet.parameter :as parameter]
   [pallet.stevedore :as stevedore]
   [pallet.compute :as compute]
   [pallet.request-map :as request-map]
   [pallet.resource.directory :as directory]
   [pallet.resource.exec-script :as exec-script]
   [pallet.resource.file :as file]
   [pallet.resource.remote-directory :as remote-directory]
   [pallet.resource.remote-file :as remote-file]
   [pallet.resource.user :as user]
   [pallet.resource.filesystem-layout :as filesystem-layout]
   [pallet.script :as script]
   [pallet.crate.java :as java]
   [clojure.contrib.prxml :as prxml]
   [clojure.string :as string]
   [clojure.contrib.logging :as log]))

(def log-path "/var/log/hadoop")
(def tx-log-path (format "%s/txlog" log-path))
(def config-path "/etc/hadoop")
(def data-path "/var/hadoop")
(def hadoop-home "/usr/local/hadoop")
(def hadoop-user "hadoop")
(def hadoop-group "hadoop")

(defn url
  "Download URL for the Apache distribution of Hadoop, generated for
  the supplied version."
  [version]
  (format
   "http://www.apache.org/dist/hadoop/core/hadoop-%s/hadoop-%s.tar.gz"
   version version))

(defn install
  "Initial hadoop installation."
  [request & {:keys [user group version home]
              :or {user hadoop-user
                   group hadoop-group
                   version "0.20.2"}
              :as options}]
  (let [url (url version)
        home (or home (format "%s-%s" hadoop-home version))
        config-dir (str home "/conf")
        etc-config-dir (stevedore/script (str (config-root) "/hadoop"))
        pid-dir (stevedore/script (str (pid-root) "/hadoop"))
        log-dir (stevedore/script (str (log-root) "/hadoop"))
        data-dir "/data"]
    (->
     request
     (parameter/assoc-for-target
      [:hadoop :home] home
      [:hadoop :owner] user
      [:hadoop :group] group
      [:hadoop :config-dir] config-dir
      [:hadoop :data-dir] data-dir
      [:hadoop :pid-dir] pid-dir
      [:hadoop :log-dir] log-dir)
     (user/user user :system true)
     (user/group group :system true)
     (remote-directory/remote-directory
      home
      :url url :md5-url (str url ".md5")
      :unpack :tar :tar-options "xz"
      :owner user :group group)
     (directory/directory log-path :owner user :group group :mode "0755")
     (directory/directory config-dir :owner user :group group :mode "0755")
     (directory/directory data-dir :owner user :group group :mode "0755")
     (directory/directory pid-dir :owner user :group group :mode "0755")
     (directory/directory log-dir :owner user :group group :mode "0755")
     (file/symbolic-link config-dir etc-config-dir))))

(defn hadoop-filesystem-dirs
  [request root]
  (let [owner (parameter/get-for-target request [:hadoop :owner])
        group (parameter/get-for-target request [:hadoop :group])]
    (-> request
        (directory/directory
         (str root "/hadoop") :owner owner :group group)
        (directory/directory
         (str root "/hadoop/logs") :owner owner :group group)
        (directory/directory
         (str root "/tmp") :owner owner :group group :mode "a+rwxt"))))

(defn default-properties
  "Returns a nested map of default properties, named according to the
  0.20 api."
  [data-root name-node-ip job-tracker-ip owner]
  {:hdfs-site {:dfs.data.dir (str data-root "/hadoop/hdfs/data")
               :dfs.datanode.du.reserved 1073741824
               :dfs.name.dir (str data-root "/hadoop/hdfs/name")
               :dfs.namenode.handler.count 10
               :dfs.permissions.enabled true
               :dfs.replication 3}
   :mapred-site {:tasktracker.http.threads 46
                 :mapred.child.java.opts "-Xmx550m"
                 :mapred.child.ulimit 1126400
                 :mapred.job.tracker (format "%s:8021" job-tracker-ip)
                 :mapred.job.tracker.handler.count 10
                 :mapred.local.dir (str data-root "/hadoop/hdfs/mapred/local")
                 :mapred.map.tasks.speculative.execution true
                 :mapred.reduce.tasks.speculative.execution false
                 :mapred.reduce.parallel.copies 10
                 :mapred.reduce.tasks 10
                 :mapred.submit.replication 10
                 :mapred.system.dir (str data-root "/hadoop/hdfs/system/mapred")
                 :mapred.tasktracker.map.tasks.maximum 2
                 :mapred.tasktracker.reduce.tasks.maximum 1
                 :mapred.compress.map.output true
                 :mapred.output.compression.type "BLOCK"}
   :core-site {:fs.checkpoint.dir (str data-root "/hadoop/hdfs/secondary")
               :fs.default.name (format "hdfs://%s:8020/" name-node-ip)
               :fs.trash.interval 1440
               :io.file.buffer.size 65536
               :hadoop.tmp.dir (str data-root (str "/tmp/hadoop" owner))
               :hadoop.rpc.socket.factory.class.default "org.apache.hadoop.net.StandardSocketFactory"
               :hadoop.rpc.socket.factory.class.ClientProtocol ""
               :hadoop.rpc.socket.factory.class.JobSubmissionProtocol ""
               :io.compression.codecs (str
                                       "org.apache.hadoop.io.compress.DefaultCodec,"
                                       "org.apache.hadoop.io.compress.GzipCodec")}})

(def final-properties
  #{:dfs.block.size
    :dfs.data.dir
    :dfs.datanode.du.reserved
    :dfs.datanode.handler.count
    :dfs.hosts
    :dfs.hosts.exclude
    :dfs.name.dir
    :dfs.namenode.handler.count
    :dfs.permissions
    :fs.checkpoint.dir
    :fs.trash.interval
    :hadoop.tmp.dir
    :mapred.child.ulimit
    :mapred.job.tracker.handler.count
    :mapred.local.dir
    :mapred.tasktracker.map.tasks.maximum
    :mapred.tasktracker.reduce.tasks.maximum
    :tasktracker.http.threads
    :hadoop.rpc.socket.factory.class.default
    :hadoop.rpc.socket.factory.class.ClientProtocol
    :hadoop.rpc.socket.factory.class.JobSubmissionProtocol})

(defn ppxml
  "XML pretty printing, as described at
  http://nakkaya.com/2010/03/27/pretty-printing-xml-with-clojure/"
  [xml]
  (let [in (javax.xml.transform.stream.StreamSource.
            (java.io.StringReader. xml))
        writer (java.io.StringWriter.)
        out (javax.xml.transform.stream.StreamResult. writer)
        transformer (.newTransformer
                     (javax.xml.transform.TransformerFactory/newInstance))]
    (.setOutputProperty transformer
                        javax.xml.transform.OutputKeys/INDENT "yes")
    (.setOutputProperty transformer
                        "{http://xml.apache.org/xslt}indent-amount" "2")
    (.setOutputProperty transformer
                        javax.xml.transform.OutputKeys/METHOD "xml")
    (.transform transformer in out)
    (-> out .getWriter .toString)))

(defn property->xml
  "Create a nested sequence representing the XML for a property."
  [property final]
  [:property
   (filter
    identity
    [[:name {} (name (key property))]
     [:value {} (val property)]
     (when final
       [:final {} "true"])])])

(defn properties->xml
  [properties]
  (ppxml
   (with-out-str
     (prxml/prxml
      [:decl! {:version "1.0"}]
      [:configuration
       (map
        #(property->xml % (final-properties (key %)))
        properties)]))))

(defn config-files
  [request properties]
  (let [owner (parameter/get-for-target request [:hadoop :owner])
        group (parameter/get-for-target request [:hadoop :group])]
    (doseq [[filename props] properties]
      (->
       request
       (remote-file/remote-file
        (format "/%s.xml" (name filename))
        :content (properties->xml props)
        :owner owner :group group)))))

(defn merge-config
  "Takes a map of Hadoop configuration options and merges in the
  supplied map of custom configuration options."
  [default-props
  new-props]
  (apply merge
         (for [[name props] default-props]
           {name (merge props (name new-props))})))

(defn format-exports [export-map]
  (string/join
   (map (fn [[k v]]
          (format "export %s=%s\n" (name k) v))
        export-map)))

(defn env-file
  [request]
  (let [pid-dir (parameter/get-for-target request [:hadoop :pid-dir])
        log-dir (parameter/get-for-target request [:hadoop :log-dir])
        config-dir (parameter/get-for-target request [:hadoop :config-dir])]
    (->
     request
     (remote-file/remote-file
      (str config-dir "/hadoop-env.sh")
      :content
      (format-exports
       {:HADOOP_PID_DIR pid-dir
        :HADOOP_SSH_OPTS "\"-o StrictHostKeyChecking=no\""
        :HADOOP_OPTS "\"-Djava.net.preferIPv4Stack=true\""
        :HADOOP_LOG_DIR (str log-dir "/logs")})))))

(defn get-master-ip
  "Returns the private IP address of a particular type of master node,
  as defined by tag. Logs a warning if more than one master exists."
  [request tag]
  (let [[master :as nodes] (request-map/nodes-in-tag request name)
        kind (name tag)]
    (when (> (count nodes) 1)
      (log/warn (format "There are more than one %s" kind)))
    (if-not master
      (log/error (format "There is no %s defined!" kind))
      (compute/private-ip name-node))))

(defn configure
  "Configure Hadoop cluster, with custom properties."
  [request data-root name-node-tag job-tracker-tag {:as properties}]
  (let [log-dir (parameter/get-for-target request [:hadoop :log-dir])
        owner (parameter/get-for-target request [:hadoop :owner])
        name-node-ip (get-master-ip request name-node-tag)
        job-tracker-ip (get-master-ip request job-tracker-tag)
        defaults  (default-properties
                    data-root name-node-ip job-tracker-ip owner)
        properties (merge-config defaults properties)]
    (->
     request
     (hadoop-filesystem-dirs data-root)
     (config-files properties)
     env-file
     (file/symbolic-link (str data-root "/hadoop/logs") log-dir))))

(script/defscript as-user [user & command])
(stevedore/defimpl as-user :default [user & command]
  (su -s "/bin/bash" ~user
      -c "\"" (str "export JAVA_HOME=" (java-home) ";") ~@command "\""))
(stevedore/defimpl as-user [#{:yum}] [user & command]
  ("/sbin/runuser" -s "/bin/bash" - ~user -c ~@command))

(defn- hadoop-service
  "Run a Hadoop service"
  [request hadoop-daemon description]
  (let [hadoop-home (parameter/get-for-target request [:hadoop :home])
        hadoop-user (parameter/get-for-target request [:hadoop :owner])]
    (->
     request
     (exec-script/exec-checked-script
      (str "Start Hadoop " description)
      (as-user
       ~hadoop-user
       ~(str hadoop-home "/bin/hadoop-daemon.sh")
       "start"
       ~hadoop-daemon)))))

(defn- hadoop-command
  "Run a Hadoop command"
  [request & args]
  (let [hadoop-home (parameter/get-for-target request [:hadoop :home])
        hadoop-user (parameter/get-for-target request [:hadoop :owner])]
    (->
     request
     (exec-script/exec-checked-script
      (str "hadoop " args)
      (as-user
       ~hadoop-user
       ~(str hadoop-home "/bin/hadoop")
       ~@args)))))

(defn name-node
  "Run a Hadoop name node"
  [request data-dir]
  (let [hadoop-home (parameter/get-for-target request [:hadoop :home])
        hadoop-user (parameter/get-for-target request [:hadoop :owner])]
    (->
     request
     ;; Format FS only if it hasn't been already formatted. Otherwise
     ;; say no.
     (exec-script/exec-script
      (as-user ~hadoop-user
               (pipe
                (echo "N")
                (~(str hadoop-home "/bin/hadoop") "namenode" "-format"))))
     ;; start name node service
     (hadoop-service "namenode" "Name Node")
     (hadoop-command "dfsadmin" "-safemode" "wait")
     (hadoop-command "fs" "-mkdir" data-dir)
     (hadoop-command "fs" "-chmod" "+w" data-dir))))

(defn secondary-name-node
  "Run a Hadoop secondary name node"
  [request]
  (hadoop-service request "secondarynamenode" "secondary name node"))

(defn job-tracker
  "Run a Hadoop job tracker"
  [request]
  (-> request
      (hadoop-service "jobtracker" "job tracker")
      #_(parameter/assoc-for-service
         [:hadoop :mapred :job-tracker]
         (format "%s:8021" (request-map/target-ip)))))

(defn data-node
  "Run a Hadoop data node"
  [request]
  (-> request
      (hadoop-service "datanode" "data node")))

(defn task-tracker
  "Run a Hadoop task tracker"
  [request]
  (hadoop-service request "tasktracker" "task tracker"))