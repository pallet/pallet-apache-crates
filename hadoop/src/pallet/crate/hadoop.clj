;; -*- Mode: Clojure; indent-tabs-mode: nil -*-
;;
;; Licensed to the Apache Software Foundation (ASF) under one or more
;; contributor license agreements.  See the NOTICE file distributed
;; with this work for additional information regarding copyright
;; ownership.  The ASF licenses this file to you under the Apache
;; License, Version 2.0 (the "License"); you may not use this file
;; except in compliance with the License.  You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0 Unless
;; required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
;; implied.  See the License for the specific language governing
;; permissions and limitations under the License.

;; http://www.michael-noll.com/tutorials/running-hadoop-on-ubuntu-linux-multi-node-cluster/
;; http://wiki.apache.org/hadoop/GettingStartedWithHadoop
;; http://www.michael-noll.com/tutorials/running-hadoop-on-ubuntu-linux-single-node-cluster
;; http://hadoop.apache.org/mapreduce/docs/current/mapred-default.html
;; http://hadoop.apache.org/hdfs/docs/current/hdfs-default.html
;; http://hadoop.apache.org/common/docs/current/cluster_setup.html#core-site.xml
;; http://hadoop.apache.org/#What+Is+Hadoop%3F

(ns pallet.crate.hadoop
  "Pallet crate to manage Hadoop installation and configuration.
INCOMPLETE - not yet ready for general use."
  (:use [pallet.thread-expr :only (apply->)]
        [pallet.resource :only (phase)])
  (:require [pallet.parameter :as parameter]
            [pallet.stevedore :as stevedore]
            [pallet.compute :as compute]
            [pallet.request-map :as request-map]
            [pallet.resource.directory :as directory]
            [pallet.resource.exec-script :as exec-script]
            [pallet.resource.file :as file]
            [pallet.resource.remote-directory :as remote-directory]
            [pallet.resource.remote-file :as remote-file]
            [pallet.resource.user :as user]
            [pallet.script :as script]
            [pallet.crate.java :as java]
            [clojure.contrib.prxml :as prxml]
            [clojure.string :as string]
            [clojure.contrib.logging :as log]
            [pallet.crate.ssh-key :as ssh-key]))

;; This crate contains all information required to set up and
;; configure a fully functional installation of Apache's
;; Hadoop. Working through this crate, you might find Michael
;; G. Noll's [single node](http://goo.gl/8ogSk) and [multiple
;; node](http://goo.gl/NIWoK) hadoop cluster tutorials to be helpful.

(def default-home "/usr/local/hadoop")
(def default-user "hadoop")
(def default-group "hadoop")
(def default-version "0.20.2")

;; ### Utilities

(defmacro for->
  "Custom version of for->, with support for destructuring."
  [arg seq-exprs body-expr]
  `((apply comp (reverse
                 (for ~seq-exprs
                   (fn [arg#]
                     (-> arg#
                         ~body-expr)))))
    ~arg))

;; ### Hadoop Utils

(defn url
  "Download URL for the Apache distribution of Hadoop, generated for
  the supplied version."
  [version]
  (format
   "http://www.apache.org/dist/hadoop/core/hadoop-%s/hadoop-%s.tar.gz"
   version version))

(defn format-exports
  "Formats `export` lines for inclusion in a shell script."
  [& kv-pairs]
  (string/join
   (for [[k v] (partition 2 kv-pairs)]
     (format "export %s=%s\n" (name k) v))))

(defn hadoop-param
  "Pulls the value referenced by the supplied key out of the supplied
  hadoop cluster request map."
  [request key]
  (parameter/get-for-target request [:hadoop key]))

(defn remote-file
  "Remote file implementation for hadoop install TODO -- why are we
  abstracting this out?."
  [request path content]
  (let [owner (hadoop-param request :owner)
        group (hadoop-param request :group)]
    (remote-file/remote-file path
                             :content content
                             :owner owner
                             :group group)))

;; ### User Creation

(defn create-hadoop-user
  "Create the hadoop user"
  [request]
  (let [user (hadoop-param request :owner)
        group (hadoop-param request :group)
        hadoop-home (hadoop-param request :home)
        jdk-home (stevedore/script (java/java-home))]
    (-> request
        (user/group group :system true)
        (user/user user
                   :system true
                   :create-home true
                   :shell :bash)
        (remote-file/remote-file
         (str "/home/" user "/.bash_profile")
         :owner user
         :group group
         :literal true
         :content
         (format-exports
          :JAVA_HOME jdk-home
          :PATH (format "$PATH:%s/bin" hadoop-home))))))

(defn publish-ssh-key
  "Sets up this node to be able passwordlessly ssh into the other nodes (slaves)"
  [request]
  (let [user (hadoop-param request :owner)
        id (request-map/target-id request)
        tag (request-map/tag request)
        key-name (format "%s_%s_key" tag id)]
    (-> request
        (ssh-key/generate-key user :comment key-name)
        (ssh-key/record-public-key user))))

(defn- get-node-ids-for-group
  "Get the id of the nodes in a group node"
  [request tag]
  (let [nodes (request-map/nodes-in-tag request tag)]
    (map compute/id nodes)))

(defn- get-keys-for-group
  "get the ssh for a user in a group"
  [request tag user]
  (for [node (get-node-ids-for-group request tag)]
    (parameter/get-for request
                       [:host (keyword node)
                        :user (keyword user)
                        :id_rsa])))

#_(defn authorize-group
  [request user tag users]
  (let [keys (get-keys-for-group request tag users)]
    (->
     request
     (for-> [key keys]
            (ssh-key/authorize-key user key)))))

(defn- authorize-key
  [request local-user group remote-user]
  (let [keys (get-keys-for-group request group remote-user)]
    (for-> request
           [key keys]
           (ssh-key/authorize-key local-user key))))

(defn authorize-groups
  "Authorizes the master node to ssh into this node"
  [request local-users tag-remote-users-map]
  (for-> request
         [local-user local-users
          [group remote-users] tag-remote-users-map
          remote-user remote-users
          :let [authorization [local-user group remote-user]]]
         (apply-> authorize-key authorization)))

(defn authorize-jobtracker
  [request]
  (authorize-groups request ["hadoop"] {"jobtracker" ["hadoop"]}))

;; ### Installation

(defn populate-request
  [request user group home]
  (parameter/assoc-for-target request
                              [:hadoop :owner] user
                              [:hadoop :group] group
                              [:hadoop :home] home
                              [:hadoop :owner-dir] (stevedore/script (user/user-home ~user))
                              [:hadoop :config-dir] (str home "/conf")
                              [:hadoop :data-dir] "/data"
                              [:hadoop :pid-dir] (stevedore/script (str (pid-root) "/hadoop"))
                              [:hadoop :log-dir] (stevedore/script (str (log-root) "/hadoop"))))

(defn install
  "Initial hadoop installation."
  [request & {:keys [user group version home]
              :or {user default-user
                   group default-group
                   version default-version}}]
  (let [url (url version)
        home (or home (format "%s-%s" default-home version))]
    (->
     request
     (populate-request user group home)
     (create-hadoop-user)
     (remote-directory/remote-directory home
                                        :url url
                                        :md5-url (str url ".md5")
                                        :unpack :tar
                                        :tar-options "xz"
                                        :owner user
                                        :group group)
     (for-> [path (map (partial hadoop-param request)
                       [:config-dir :data-dir :pid-dir :log-dir])]
            (directory/directory path
                                 :owner user
                                 :group group
                                 :mode "0755"))
     (file/symbolic-link (hadoop-param request :config-dir)
                         (stevedore/script (str (config-root) "/hadoop"))))))

;; ### Configuration

(defn default-properties
  "Returns a nested map of default properties, named according to the
  0.20 api."
  [owner-dir name-node-ip job-tracker-ip]
  (let [owner-subdir (partial str owner-dir)]
    {:hdfs-site {:dfs.data.dir (owner-subdir "/dfs/data")
                 :dfs.name.dir (owner-subdir "/dfs/name")
                 :dfs.datanode.du.reserved 1073741824
                 :dfs.namenode.handler.count 10
                 :dfs.permissions.enabled true
                 :dfs.replication 3}
     :mapred-site {:tasktracker.http.threads 46
                   :mapred.local.dir (owner-subdir "/mapred/local")
                   :mapred.system.dir "/hadoop/mapred/system"
                   :mapred.child.java.opts "-Xmx550m"
                   :mapred.child.ulimit 1126400
                   :mapred.job.tracker (format "%s:8021" job-tracker-ip)
                   :mapred.job.tracker.handler.count 10
                   :mapred.map.tasks.speculative.execution true
                   :mapred.reduce.tasks.speculative.execution false
                   :mapred.reduce.parallel.copies 10
                   :mapred.reduce.tasks 10
                   :mapred.submit.replication 10
                   :mapred.tasktracker.map.tasks.maximum 2
                   :mapred.tasktracker.reduce.tasks.maximum 1
                   :mapred.compress.map.output true
                   :mapred.output.compression.type "BLOCK"}
     :core-site {:fs.checkpoint.dir (owner-subdir "/dfs/secondary")
                 :fs.default.name (format "hdfs://%s:8020" name-node-ip)
                 :fs.trash.interval 1440
                 :io.file.buffer.size 65536
                 :hadoop.tmp.dir "/tmp/hadoop"
                 :hadoop.rpc.socket.factory.class.default "org.apache.hadoop.net.StandardSocketFactory"
                 :hadoop.rpc.socket.factory.class.ClientProtocol ""
                 :hadoop.rpc.socket.factory.class.JobSubmissionProtocol ""
                 :io.compression.codecs (str
                                         "org.apache.hadoop.io.compress.DefaultCodec,"
                                         "org.apache.hadoop.io.compress.GzipCodec")}}))

;; TODO -- discuss what the hell these final properties are!

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
  (letfn)
  (ppxml
   (with-out-str
     (prxml/prxml
      [:decl! {:version "1.0"}]
      [:configuration
       (map
        #(property->xml % (final-properties (key %)))
        properties)]))))

(defn config-files
  "TODO -- Creates XML configuration files for hadoop, located in the
  config directory."
  [request properties]
  (let [config-dir (hadoop-param request :config-dir)]
    (->
     request
     (for-> [[filename props] properties]
            (remote-file (format "%s/%s.xml"
                                 config-dir
                                 (name filename))
                         (properties->xml props))))))

(defn merge-config
  "Takes a map of Hadoop configuration options and merges in the
  supplied map of custom configuration options."
  [default-props new-props]
  (apply merge
         (for [[name props] default-props]
           {name (merge props (name new-props))})))

(defn env-file
  [request]
  (let [pid-dir (hadoop-param request :pid-dir)
        log-dir (hadoop-param request :log-dir)
        config-dir (hadoop-param request :config-dir)]
    (->
     request
     (remote-file (str config-dir "/hadoop-env.sh")
                  (format-exports
                   :HADOOP_PID_DIR pid-dir
                   :HADOOP_LOG_DIR log-dir
                   :HADOOP_SSH_OPTS "\"-o StrictHostKeyChecking=no\""
                   :HADOOP_OPTS "\"-Djava.net.preferIPv4Stack=true\"")))))

(defn get-master-ip
  "Returns the IP address of a particular type of master node,
  as defined by tag. IP-type can be :private or :public. Logs a
  warning if more than one master exists."
  [request ip-type tag]
  (let [[master :as nodes] (request-map/nodes-in-tag request tag)
        kind (name tag)]
    (when (> (count nodes) 1)
      (log/warn (format "There are more than one %s" kind)))
    (if-not master
      (log/error (format "There is no %s defined!" kind))
      ((case ip-type
             :private compute/private-ip
             :public compute/primary-ip) master))))

;;todo -- if we have the same tag for both, here, does that help us?

(defn configure
  "Configure Hadoop cluster, with custom properties."
  [request name-node-tag job-tracker-tag ip-type {:as properties}]
  {:pre [(contains? #{:public :private} ip-type)]}
  (let [name-node-ip (get-master-ip request ip-type name-node-tag)
        job-tracker-ip (get-master-ip request ip-type job-tracker-tag)
        owner-dir (hadoop-param request :owner-dir)
        log-dir (hadoop-param request :log-dir)
        owner (hadoop-param request :owner)
        group (hadoop-param request :group)
        defaults  (default-properties
                    owner-dir name-node-ip job-tracker-ip)
        properties (merge-config defaults properties)
        tmpdir (get-in properties [:core-site :hadoop.tmp.dir])]
    (->
     request
     (directory/directory  tmpdir
                           :owner owner
                           :group group)
     (config-files properties)
     env-file
     (file/symbolic-link (str owner-dir "/logs") log-dir))))

(script/defscript as-user [user & command])
(stevedore/defimpl as-user :default [user & command]
  (su -s "/bin/bash" ~user
      -c "\"" (str "export JAVA_HOME=" (java-home) ";") ~@command "\""))
(stevedore/defimpl as-user [#{:yum}] [user & command]
  ("/sbin/runuser" -s "/bin/bash" - ~user -c ~@command))

(defn- hadoop-service
  "Run a Hadoop service"
  [request hadoop-daemon description]
  (let [hadoop-home (hadoop-param request :home)
        hadoop-user (hadoop-param request :owner)]
    (->
     request
     (exec-script/exec-checked-script
      (str "Start Hadoop " description)
      (as-user
       ~hadoop-user
       ~(stevedore/script
         (if-not (pipe (jps)
                       (grep "-i" ~hadoop-daemon))
           ((str ~hadoop-home "/bin/hadoop-daemon.sh")
            "start"
            ~hadoop-daemon))))))))

(defn- hadoop-command
  "Runs '$ hadoop ...' on each machine in the request. Command runs
  has the hadoop user."
  [request & args]
  (let [hadoop-home (hadoop-param request :home)
        hadoop-user (hadoop-param request :owner)]
    (->
     request
     (exec-script/exec-checked-script
      (apply str "hadoop " (interpose " " args))
      (as-user
       ~hadoop-user
       (str ~hadoop-home "/bin/hadoop")
       ~@args)))))

(defn format-hdfs
  "Formats HDFS for the first time. If HDFS has already been
  formatted, does nothing."
  [request]
  (let [hadoop-home (hadoop-param request :home)
        hadoop-user (hadoop-param request :owner)]
    (->
     request
     (exec-script/exec-script
      (as-user ~hadoop-user
               (pipe
                (echo "N")
                ((str ~hadoop-home "/bin/hadoop")
                 "namenode"
                 "-format")))))))

;; TODO -- think about the dfsadmin call, etc, that's happening
;; here. is that needed? Do we need to run this mkdir?
(defn name-node
  "Run a Hadoop name node."
  [request data-dir]
  (->
   request
   (format-hdfs)
   (hadoop-service "namenode" "Name Node")
   (hadoop-command "dfsadmin" "-safemode" "wait")
   (hadoop-command "fs" "-mkdir" data-dir)
   (hadoop-command "fs" "-chmod" "+w" data-dir)))

(defn secondary-name-node
  "Run a Hadoop secondary name node"
  [request]
  (hadoop-service request "secondarynamenode" "secondary name node"))

(defn job-tracker
  "Run a Hadoop job tracker"
  [request]
  (hadoop-service request "jobtracker" "job tracker"))

(defn data-node
  "Run a Hadoop data node"
  [request]
  (hadoop-service request "datanode" "data node"))

(defn task-tracker
  "Run a Hadoop task tracker"
  [request]
  (hadoop-service request "tasktracker" "task tracker"))
