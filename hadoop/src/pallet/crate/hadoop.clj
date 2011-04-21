;; Licensed to the Apache Software Foundation (ASF) under one or more
;; contributor license agreements.  See the NOTICE file distributed
;; with this work for additional information regarding copyright
;; ownership.  The ASF licenses this file to you under the Apache
;; License, Version 2.0 (the "License"); you may not use this file
;; except in compliance with the License.  You may obtain a copy of
;; the License at http://www.apache.org/licenses/LICENSE-2.0 Unless
;; required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
;; implied.  See the License for the specific language governing
;; permissions and limitations under the License.

(ns pallet.crate.hadoop
  "Pallet crate to manage Hadoop installation and configuration."
  (:use [pallet.thread-expr :only (apply->)])
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
            [clojure.contrib.prxml :as prxml]
            [clojure.string :as string]
            [clojure.contrib.logging :as log]
            [pallet.crate.ssh-key :as ssh-key]
            [pallet.crate.java :as java])
  (:import [java.io StringReader StringWriter]
           [javax.xml.transform TransformerFactory OutputKeys]
           [javax.xml.transform.stream StreamSource StreamResult]))

;; #### General Utilities
;;
;; Updated version of `for->`, from `pallet.thread-expr`. This version
;; has support for destructuring. We'll need to update the pallet
;; dependency to take advantage of this.

(defmacro for->
  "Apply a thread expression to a sequence.
   eg.
      (-> 1
        (for-> [x [1 2 3]]
          (+ x)))
   => 7"
  [arg seq-exprs & body]
  `(reduce #(%2 %1)
           ~arg
           (for ~seq-exprs
             (fn [arg#] (-> arg# ~@body)))))

;; This one is generally quite useful; is there a place for this in
;; stevedore?

(defn format-exports
  "Formats `export` lines for inclusion in a shell script."
  [& kv-pairs]
  (string/join
   (for [[k v] (partition 2 kv-pairs)]
     (format "export %s=%s\n" (name k) v))))

;; ## Hadoop Configuration
;;
;; This crate contains all information required to set up and
;; configure a fully functional installation of Apache's Hadoop. It
;; seems that the biggest roadblock potential hadoop adopters face is
;; the confounding, terrible multiplicity of possible
;; configurations. Tom White, in the wonderful [Hadoop: The Definitive
;; Guide](http://goo.gl/nPWWk), states the case well: "Hadoop has a
;; bewildering number of configuration properties".
;;
;; We aim to provide sane, intelligent defaults that adjust themselves
;; based on a given cluster's size and particular distribution of
;; machines.
;;
;; The following phases are informed to some degree by Hadoop's
;; official [Getting Started](http://goo.gl/Bh4zU) page... I have to
;; say, though, Michael G. Noll's [single node](http://goo.gl/8ogSk)
;; and [multiple node](http://goo.gl/NIWoK) hadoop cluster tutorials
;; were immensely helpful.

;; ### Hadoop Defaults
;;
;; For this first version of the crate, we chose to lock down a few
;; parameters that'll be customizable down the road. (We've got a few
;; ideas on how to provide default overrides in a clean way, using
;; environments. Stay tuned!) In particular, we lock down version,
;; Hadoop's final location, and the name of the hadoop user and group
;; to be installed on each machine in the cluster.

(defn versioned-home
  "Default Hadoop location, based on version number."
  [version]
  (format "/usr/local/hadoop-%s" version))

(def default-version "0.20.2")
(def hadoop-home (versioned-home default-version))
(def hadoop-user "hadoop")
(def hadoop-group "hadoop")

;; ### User Creation
;;
;; For the various nodes of a hadoop cluster to communicate with one
;; another, they need to share a common user with common
;; permissions. Something to keep in mind when manually logging in to
;; nodes -- hadoop java processes will run as the `hadoop` user, so
;; calls to `jps` as anyone else will show nothing running. If you'd
;;like to run a test job, ssh into the machine and run
;;
;;    `sudo su - hadoop`
;;
;; before interacting with hadoop.

(defn create-hadoop-user
  "Create a hadoop user on a cluster node. We add the hadoop binary
  directory and a `JAVA_HOME` setting to `$PATH` to facilitate
  development when manually logged in to some particular node."
  [request]
  (-> request 
      (user/group hadoop-group :system true)
      (user/user hadoop-user
                 :system true
                 :create-home true
                 :shell :bash)
      (remote-file/remote-file (format "/home/%s/.bash_profile" hadoop-user)
                               :owner hadoop-user
                               :group hadoop-group
                               :literal true
                               :content (format-exports
                                         :JAVA_HOME (stevedore/script (java/java-home))
                                         :PATH (format "$PATH:%s/bin" hadoop-home)))))


;; Once the hadoop user is created, we create an ssh key for that user
;; and share it around the cluster. The jobtracker needs passwordless
;; ssh access into every cluster node running a task tracker, so that
;; it can distribute the data processing code that these machines need
;; to do anything useful.

(defn- get-node-ids-for-group
  "Get the id of the nodes in a group node"
  [request tag]
  (let [nodes (request-map/nodes-in-tag request tag)]
    (map compute/id nodes)))

(defn- get-keys-for-group
  "Returns the ssh key for a user in a group"
  [request tag user]
  (for [node (get-node-ids-for-group request tag)]
    (parameter/get-for request [:host (keyword node)
                                :user (keyword user)
                                :id_rsa])))

(defn- authorize-key
  [request local-user group remote-user]
  (let [keys (get-keys-for-group request group remote-user)]
    (for-> request [key keys]
           (ssh-key/authorize-key local-user key))))

(defn authorize-groups
  "Authorizes the master node to ssh into this node."
  [request local-users tag-remote-users-map]
  (-> request
      (for-> [local-user local-users
              [group remote-users] tag-remote-users-map
              remote-user remote-users]
             (authorize-key local-user group remote-user))))

;; In the current iteration, `publish-ssh-key` phase should only be
;; called on the job-tracker, and will only work with a subsequent
;; `authorize-jobtracker` phase on the same request. Pallet is
;; stateless between transactions, and the ssh key needs some way to
;; get between nodes. Currently, we store the new ssh key in the request.

(defn publish-ssh-key
  [request]
  (let [id (request-map/target-id request)
        tag (request-map/tag request)
        key-name (format "%s_%s_key" tag id)]
    (-> request
        (ssh-key/generate-key hadoop-user :comment key-name)
        (ssh-key/record-public-key hadoop-user))))

(defn authorize-jobtracker
  "configures all nodes to accept passwordless ssh requests from the
  jobtracker."
  [request]
  (authorize-groups request [hadoop-user] {"jobtracker" [hadoop-user]}))

;; ### Installation

(defn url
  "Hadoop download URL, generated for the supplied
  distribution. Currently supported options are `:cloudera` (CDH3) and
  `:apache` (0.20.2)."
  ([] (url :cloudera))
  ([distro]
     (case distro
           :cloudera (format
                      "http://archive.cloudera.com/cdh/3/hadoop-%s-cdh3u0.tar.gz" default-version)
           :apache (format
                    "http://www.apache.org/dist/hadoop/core/hadoop-%s/hadoop-%s.tar.gz"
                    default-version default-version))))


(defn install
  "First phase to be called when configuring a hadoop cluster. This
  phase creates a common hadoop user, and downloads and unpacks the
  default Cloudera hadoop distribution."
  [request build]
  (let [url (url build)]
    (-> request
        create-hadoop-user
        (remote-directory/remote-directory hadoop-home
                                           :url url
                                           :unpack :tar
                                           :tar-options "xz"
                                           :owner hadoop-user
                                           :group hadoop-user))))

;; ### Configuration
;;
;; Hadoop has three main configuration files, each of which are a
;; series of key-value pairs, stored as XML files. Before cluster
;; configuration, we need some way to pretty-print human readable XML
;; representing the configuration properties that we'll store in a
;; clojure map.

(defn ppxml
  "Accepts an XML string with no newline formatting and returns the
 same XML with pretty-print formatting, as described by Nurullah Akaya
 in [this post](http://goo.gl/Y9OVO)."
  [xml-str]
  (let [in  (StreamSource. (StringReader. xml-str))
        out (StreamResult. (StringWriter.))
        transformer (.newTransformer
                     (TransformerFactory/newInstance))]
    (doseq [[prop val] {OutputKeys/INDENT "yes"
                        OutputKeys/METHOD "xml"
                        "{http://xml.apache.org/xslt}indent-amount" "2"}]
      (.setOutputProperty transformer prop val))
    (.transform transformer in out)
    (str (.getWriter out))))

(defn property->xml
  "Returns a nested sequence representing the XML for a hadoop
  configuration property. if `final?` is true, `<final>true</final>`
  is added to the XML entry, preventing any hadoop job from overriding
  the property."
  [property final?]
  [:property (filter
              identity
              [[:name {} (name (key property))]
               [:value {} (val property)]
               (when final?
                 [:final {} "true"])])])

(declare final-properties)

(defn properties->xml
  "Converts a map of [property value] entries into a string of XML
  with pretty-print formatting."
  [properties]
  (ppxml
   (with-out-str
     (prxml/prxml
      [:decl! {:version "1.0"}]
      [:configuration
       (map
        #(property->xml % (final-properties (key %)))
        properties)]))))


;; ### Sane Defaults
;;
;; As mentioned before, Hadoop configuration can be a bit
;; bewildering. Default values and descriptions of the meaning of each
;; setting can be found here:
;;
;; http://hadoop.apache.org/core/docs/r0.20.0/mapred-default.html
;; http://hadoop.apache.org/core/docs/r0.20.0/hdfs-default.html
;; http://hadoop.apache.org/core/docs/r0.20.0/core-default.html
;;
;; We override a number of these below based on suggestions found in
;; various posts. We'll supply more information on the justification
;; for each of these as we move forward with our dynamic "sane
;; defaults" system.

(defn default-properties
  "Returns a nested map of Hadoop default configuration properties,
  named according to the 0.20 api."
  [name-node-ip job-tracker-ip pid-dir log-dir]
  (let [owner-dir (stevedore/script (user/user-home ~hadoop-user))
        owner-subdir (partial str owner-dir)]
    {:hdfs-site {:dfs.data.dir (owner-subdir "/dfs/data")
                 :dfs.name.dir (owner-subdir "/dfs/name")
                 :dfs.datanode.du.reserved 1073741824
                 :dfs.namenode.handler.count 10
                 :dfs.permissions.enabled true
                 :dfs.replication 3
                 :dfs.datanode.max.xcievers 4096}
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
                   :mapred.reduce.tasks 5
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
                                         "org.apache.hadoop.io.compress.GzipCodec")}
     :hadoop-env {:HADOOP_PID_DIR pid-dir
                  :HADOOP_LOG_DIR log-dir
                  :HADOOP_SSH_OPTS "\"-o StrictHostKeyChecking=no\""
                  :HADOOP_OPTS "\"-Djava.net.preferIPv4Stack=true\""}}))

;; Final properties are properties that can't be overridden during the
;; execution of a job. We're not sure that these are the right
;; properties to lock, as of now -- this will become more clear as we
;; move forward with sane defaults. In the meantime, any suggestions
;; would be much appreciated.

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

(defn config-files
  "Accepts a base directory and a map of [config-filename,
property-map] pairs, and augments the supplied request to allow for
the creation of each referenced configuration file within the base
directory."
  [request config-dir properties]
  (-> request
      (for-> [[filename props] properties]
             (remote-file/remote-file
              (format "%s/%s.xml" config-dir (name filename))
              :content (properties->xml props)
              :owner hadoop-user :group hadoop-group))))

(def merge-config (partial merge-with merge))

(defn merge-and-split-config
  "Merges a set of custom hadoop configuration option maps into the
  current defaults, and returns a 2-vector where the first item is a
  map of *-site files, and the second item is a map of exports for
  `hadoop-env.sh`. If a conflict exists, entries in `new-props` knock
  out entries in `default-props`."
  [default-props new-props]
  (let [prop-map (merge-config default-props new-props)
        corekey-seq [:core-site :hdfs-site :mapred-site]
        envkey-seq [:hadoop-env]]
    (map #(select-keys prop-map %) [corekey-seq envkey-seq])))

(defn env-file
  "Phase that creates the `hadoop-env.sh` file with references to the
  supplied pid and log dirs. `hadoop-env.sh` will be placed within the
  supplied config directory."
  [request config-dir env-map]
  (-> request
      (for-> [[fname exports] env-map
              :let [fname (name fname)
                    export-seq (flatten (seq exports))]]
             (remote-file/remote-file
              (format "%s/%s.sh" config-dir fname)
              :content (apply format-exports export-seq)))))

;; We do our development on local machines using `vmfest`, which
;; brought us in context with the next problem. Some clouds --
;; Amazon's EC2, for example -- require nodes to be configured with
;; private IP addresses. Hadoop is designed for use within private
;; clusters, so this is typically the right choice. Sometimes,
;; however, public IP addresses are preferable, as in a virtual
;; machine setup.
;;
;; Hadoop takes the IP addresses in `fs.default.name`,
;; `mapred.job.tracker` and performs a reverse DNS lookup, tracking
;; each machine by its hostname. If your cluster isn't set up to
;; handle DNS lookup, you might run into some interesting issues. On
;; these VMs, for example, reverse DNS lookups by the virtual machines
;; on each other caused every VM to resolve to the hostname of my home
;; router. This can cause jobs to limp along or fail msyteriously. We
;; have a workaround involved the `/etc/hosts` file planned for a
;; future iteration.

(defn get-master-ip
  "Returns the IP address of a particular type of master node,
  as defined by tag. IP-type can be `:private` or `:public`. Function
  logs a warning if more than one master exists."
  [request ip-type tag]
  {:pre [(contains? #{:public :private} ip-type)]}
  (let [[master :as nodes] (request-map/nodes-in-tag request tag)
        kind (name tag)]
    (when (> (count nodes) 1)
      (log/warn (format "There are more than one %s" kind)))
    (if-not master
      (log/error (format "There is no %s defined!" kind))
      ((case ip-type
             :private compute/private-ip
             :public compute/primary-ip) master))))

(defn configure
  "Configures a Hadoop cluster by creating all required default
  directories, and populating the proper configuration file
  options. The `properties` parameter must be a map of the form

    {:core-site {:key val...}
     :hdfs-site {:key val ...}
     :mapred-site {:key val ...}
     :hadoop-env {:export val ...}}

  No other top-level keys are supported at this time."
  [request ip-type namenode-tag jobtracker-tag properties]
  (let [conf-dir (str hadoop-home "/conf")
        etc-conf-dir (stevedore/script
                      (str (config-root) "/hadoop"))
        nn-ip (get-master-ip request ip-type namenode-tag)
        jt-ip (get-master-ip request ip-type jobtracker-tag)
        pid-dir (stevedore/script (str (pid-root) "/hadoop"))
        log-dir (stevedore/script (str (log-root) "/hadoop"))
        defaults  (default-properties nn-ip jt-ip pid-dir log-dir)
        [props env] (merge-and-split-config defaults properties)
        tmp-dir (get-in properties [:core-site :hadoop.tmp.dir])]
    (-> request
        (for-> [path  [conf-dir tmp-dir log-dir pid-dir]]
               (directory/directory path
                                    :owner hadoop-user
                                    :group hadoop-group
                                    :mode "0755"))
        (file/symbolic-link conf-dir etc-conf-dir)
        (config-files conf-dir props)
        (env-file conf-dir env))))

;; The following script allows for proper transmission of SSH
;; commands, with hadoop's required `JAVA_HOME` property all set.

(script/defscript as-user [user & command])
(stevedore/defimpl as-user :default [user & command]
  (su -s "/bin/bash" ~user
      -c "\"" (str "export JAVA_HOME=" (java-home) ";") ~@command "\""))
(stevedore/defimpl as-user [#{:yum}] [user & command]
  ("/sbin/runuser" -s "/bin/bash" - ~user -c ~@command))

;; Hadoop services, or `roles`, are all run by the `hadoop-daemon.sh`
;; command. Other scripts exist, such as `hadoop-daemons.sh` (for
;; running commands on many nodes at once), but pallet takes over for
;; a good number of these. The following `phase-fn` takes care to only
;; start a hadoop service that's not already running for the `hadoop`
;; user. Future iterations may provide the ability to force some
;; daemon service to restart.

(defn hadoop-service
  "Run a Hadoop service"
  [request hadoop-daemon description]
  (exec-script/exec-checked-script
   request
   (str "Start Hadoop " description)
   (as-user
    ~hadoop-user
    ~(stevedore/script
      (if-not (pipe (jps)
                    (grep "-i" ~hadoop-daemon))
        ((str ~hadoop-home "/bin/hadoop-daemon.sh")
         "start"
         ~hadoop-daemon))))))

(defn hadoop-command
  "Runs '$ hadoop `args`' on each machine in the request. Command runs
  as the hadoop user."
  [request & args]
  (exec-script/exec-checked-script
   request
   (apply str "hadoop " (interpose " " args))
   (as-user
    ~hadoop-user
    (str ~hadoop-home "/bin/hadoop")
    ~@args)))

;; `format-hdfs` is, effectively, a call to
;;
;;    `(hadoop-command "namenode" "-format")
;;
;; that call would only work the first time, however. On subsequent
;;format requests, hadoop tells the user that the namenode has already
;;been formatted, and asks for confirmation. The current version of
;;`format-namenode` sends a default "N" every time.

(defn format-hdfs
  "Formats HDFS for the first time. If HDFS has already been
  formatted, does nothing."
  [request]
  (exec-script/exec-script
   request
   (as-user ~hadoop-user
            (pipe
             (echo "N")
             ((str ~hadoop-home "/bin/hadoop")
              "namenode"
              "-format")))))

;; And, here we are at the end! The following five functions activate
;; each of the five distinct roles that hadoop nodes may take on.

(defn name-node
  "Collection of all subphases required for a namenode."
  [request data-dir]
  (-> request
      format-hdfs
      (hadoop-service "namenode" "Name Node")
      (hadoop-command "dfsadmin" "-safemode" "wait")
      (hadoop-command "fs" "-mkdir" data-dir)
      (hadoop-command "fs" "-chmod" "+w" data-dir)))

(defn secondary-name-node
  [request]
  (hadoop-service request "secondarynamenode" "secondary name node"))

(defn job-tracker
  [request]
  (hadoop-service request "jobtracker" "job tracker"))

(defn data-node
  [request]
  (hadoop-service request "datanode" "data node"))

(defn task-tracker
  [request]
  (hadoop-service request "tasktracker" "task tracker"))
