;; -*- Mode: Clojure; indent-tabs-mode: nil -*-
;;
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
  "Pallet crate to manage Hadoop installation and configuration.
INCOMPLETE - not yet ready for general use, but close!"
  (:use [pallet.thread-expr :only (apply->)]
        [clojure.contrib.def :only (name-with-attributes)])
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
            [pallet.crate.ssh-key :as ssh-key]
            [clojure.contrib.condition :as condition]
            [clojure.contrib.macro-utils :as macro])
  (:import [java.io StringReader StringWriter]
           [javax.xml.transform TransformerFactory OutputKeys]
           [javax.xml.transform.stream StreamSource StreamResult]))

;; This crate contains all information required to set up and
;; configure a fully functional installation of Apache's
;; Hadoop. Working through this crate, you might find Michael
;; G. Noll's [single node](http://goo.gl/8ogSk) and [multiple
;; node](http://goo.gl/NIWoK) hadoop cluster tutorials to be helpful.

;; Other helpful links:
;;
;; http://www.michael-noll.com/tutorials/running-hadoop-on-ubuntu-linux-multi-node-cluster/
;; http://wiki.apache.org/hadoop/GettingStartedWithHadoop
;; http://www.michael-noll.com/tutorials/running-hadoop-on-ubuntu-linux-single-node-cluster
;; http://hadoop.apache.org/mapreduce/docs/current/mapred-default.html
;; http://hadoop.apache.org/hdfs/docs/current/hdfs-default.html
;; http://hadoop.apache.org/common/docs/current/cluster_setup.html#core-site.xml
;; http://hadoop.apache.org/#What+Is+Hadoop%3F

;; ### Hadoop Defaults
;;
;; Here's a method for creating default directory names based on
;;version, if you're so inclined.

(defn versioned-home
  "Default Hadoop location, based on version number."
  [version]
  (format "/usr/local/hadoop-%s" version))

;; Sane defaults, until we implement defaults using the environment
;; system.

(def default-version "0.20.2")
(def hadoop-home (versioned-home default-version))
(def hadoop-user "hadoop")
(def hadoop-group "hadoop")

;; ### Pallet Utils
;; TODO -- we can remove these, if Hugo adds them to pallet.

;; #### Threading Macros

(defmacro for->
  "Custom version of for->, with support for destructuring."
  [arg seq-exprs body-expr]
  `((apply comp (reverse
                 (for ~seq-exprs
                   (fn [param#]
                     (-> param# ~body-expr)))))
    ~arg))

(defmacro let->
  "Allows binding of variables in a threading expression, with support
  for destructuring. For example:

 (-> 5
    (let-> [x 10]
           (for-> [y (range 4)
                   z (range 2)] (+ x y z))))
 ;=> 101"
  [arg letvec & body]
  `(let ~letvec (-> ~arg ~@body)))

(defmacro expose-arg->
  "A threaded form that exposes the value of the threaded arg. For
  example:

    (-> 1
      (expose-arg-> [arg]
        (+ arg)))
  ;=> 2"
  [arg [sym] & body]
  `(let [~sym ~arg] (-> ~sym ~@body)))

(defmacro let-with-arg->
  "A `let` form that can appear in a request thread, and assign the
   value of the threaded arg.

   eg.
      (-> 1
        (let-with-arg-> val [a 1]
          (+ a val)))
   => 3"
  [arg sym binding & body]
  `(let [~sym ~arg
         ~@binding]
     (-> ~sym ~@body)))

;; TODO -- add apply, if... what else?
(defmacro -->
  "Similar to `clojure.core/->`, but includes symbol macros  for `when`,
  `let` and `for` commands on the internal threading expressions."
  [& forms]
  `(macro/symbol-macrolet
    [~'when pallet.thread-expr/when->
     ~'for for->
     ~'let let->
     ~'expose-request-as expose-arg->]
    (-> ~@forms)))

;; #### Phase Macros

(defn check-session
  "Function that can check a session map to ensure it is a valid part of
   phase definition. It returns the session map.

   On failure, the function will print the phase through which the
   session passed prior to crashing. It is like that this phase
   introduced the fault into the session; to aid in debugging, make
   sure to use `phase-fn` and `def-phase-fn` to build phases."
  [session form]
  (when (and session (map? session))
    session
    (condition/raise
     :type :invalid-session
     :message
     (str
      "Invalid session map in phase.\n"
      (format "session is %s\n" session)
      (format "Problem probably caused by subphase:\n  %s\n" form)
      "Check for non crate functions, improper crate functions, or
      problems in threading the session map in your phase
      definition. A crate function is a function that takes a session
      map and other arguments, and returns a modified session
      map. Calls to crate functions are often wrapped in a threading
      macro, -> or pallet.phase/phase-fn, to simplify chaining of the
      session map argument."))))

;; To qualify as a checker, a function must take two arguments -- the
;; threaded argument, and a description of the form from which it's
;; just returned.

;; TODO -- have this thing do arglists properly.
(defmacro defthreadfn
  "Binds a var to a particular class of anonymous functions that
  accept a vector of arguments and a number of subexpressions. When
  calling the produced functions, the caller must supply a threading
  parameter as the first argument. This parameter will be threaded
  through the resulting forms; the extra parameters made available in
  the argument vector will be available to all subexpressions.

  In addition to the argument vector, `defthreadfn` accepts any number
  of watchdog functions, each of which are inserted into the thread
  after each subexpression invocation. These watchdog functions are
  required to accept 2 arguments -- the threaded argument, and a
  string representation of the previous subexpression. For example,

    (defthreadfn mapped-thread-fn
      (fn [arg sub-expr]
        (if (map? arg)
          arg
          (condition/raise
           :type :invalid-session
           :message
           (format \"Only maps are allowed, and %s is most definitely
                   not a map.\" sub-expr)))))

  Returns a `thread-fn` that makes sure the threaded argument remains
  a map, on every step of its journey.

  For a more specific example if `defthreadfn`, see
  `pallet.phase/phase-fn`."
  [macro-name & rest]
  (let [[macro-name checkers] (name-with-attributes macro-name rest)]
    `(defmacro ~macro-name
       ([argvec#] (~macro-name argvec# identity))
       ([argvec# subphase# & left#]
          `(fn [~'session# ~@argvec#]
             (--> ~'session#
                 ~subphase#
                 ~@(for [func# ~(vec checkers)]
                     (list func# (str subphase#)))
                 ~@(when left#
                     [`((~'~macro-name ~argvec# ~@left#) ~@argvec#)])))))))

(defthreadfn phase-fn
  "Composes a phase function from a sequence of phases by threading an
 implicit phase session parameter through each. Each phase will have
 access to the parameters passed in through `phase-fn`'s argument
 vector. thus,

    (phase-fn [filename]
         (file filename)
         (file \"/other-file\"))

   is equivalent to:

   (fn [session filename]
     (-> session
         (file filename)
         (file \"/other-file\")))
  
   with a number of verifications on the session map performed after
   each phase invocation."
  check-session)

(defthreadfn unchecked-phase-fn
  "Unchecked version of `phase-fn`.

   The following two forms are equivalent:

   (unchecked-phase-fn [x y]
       (+ x)
       (+ y))

   (fn [session x y]
       (-> session
           (+ x)
           (+ y)))")

;; TODO -- Support for various arg lists?
(defmacro def-phase-fn
  "Binds a `phase-fn` to the supplied name."
  [name & rest]
  (let [[name [argvec & body]]
        (name-with-attributes name rest)]
    `(def ~name
       (phase-fn ~argvec ~@body))))

;; ### Hadoop Utils

(defn format-exports
  "Formats `export` lines for inclusion in a shell script."
  [& kv-pairs]
  (string/join
   (for [[k v] (partition 2 kv-pairs)]
     (format "export %s=%s\n" (name k) v))))

;; ## User Creation
;;
;; For the various nodes of a hadoop cluster to communicate with one
;; another, it becomes necessary to create a common user account on
;; every machine. For this iteration of the crate, we chose the
;; user/group pair of `hadoop/hadoop`, as defined by `hadoop-user` and
;; `hadoop-group`.

(def-phase-fn create-hadoop-user
  "Create a hadoop user on a cluster node. We add the hadoop binary
  directory and a `JAVA_HOME` setting to `$PATH` to facilitate
  development when manually logged in to some particular node."
  []
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
                                     :PATH (format "$PATH:%s/bin" hadoop-home))))


;; Once the hadoop user is created, we need to create an ssh key for
;; that user and share it around the cluster. The Jobtracker in
;; particular needs the ability to ssh without a password into every
;; cluster node running a task tracker. We assume in the following
;; functions that this could be any node other than the jobtracker.

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

;; TODO -- delete?
#_(defn authorize-group
  [request user tag users]
  (let [keys (get-keys-for-group request tag users)]
    (->
     request
     (for-> [key keys]
            (ssh-key/authorize-key user key)))))

;; TODO -- convert into phase -- define private phase?
(defn- authorize-key
  [request local-user group remote-user]
  (let [keys (get-keys-for-group request group remote-user)]
    (for-> request
           [key keys]
           (ssh-key/authorize-key local-user key))))

;; TODO -- Convert into phase, after adding apply-> to -->.
(defn authorize-groups
  "Authorizes the master node to ssh into this node."
  [request local-users tag-remote-users-map]
  (for-> request
         [local-user local-users
          [group remote-users] tag-remote-users-map
          remote-user remote-users
          :let [authorization [local-user group remote-user]]]
         (apply-> authorize-key authorization)))

;; In the current iteration, `publish-ssh-key` phase should only be
;; called on the job-tracker, and will only work with a subsequent
;; `authorize-jobtracker` phase on the same request.

(def-phase-fn publish-ssh-key
  []
  (expose-request-as [request]
   (let [id (request-map/target-id request)
         tag (request-map/tag request)
         key-name (format "%s_%s_key" tag id)]
     (ssh-key/generate-key hadoop-user :comment key-name)
     (ssh-key/record-public-key hadoop-user))))

(def-phase-fn authorize-jobtracker
  "configures all nodes to accept passwordless ssh requests from the
  jobtracker."
  []
  (authorize-groups [hadoop-user] {"jobtracker" [hadoop-user]}))

;; ### Installation
;;
(defn url
  "Download URL for the Apache distribution of Hadoop, generated for
  the supplied version."
  [version]
  (format
   "http://www.apache.org/dist/hadoop/core/hadoop-%s/hadoop-%s.tar.gz"
   version version))

(def-phase-fn install
  "Initial hadoop installation."
  []
  (let [url (url default-version)]
    create-hadoop-user
    (remote-directory/remote-directory hadoop-home
                                       :url url
                                       :md5-url (str url ".md5")
                                       :unpack :tar
                                       :tar-options "xz"
                                       :owner hadoop-user
                                       :group hadoop-user)))

;; ## Configuration
;;
;;
;;
;; TODO -- talk about how we're providing facilities for printing the
;;configuration files out as XML. Talk a bit about how complicated
;;configuration can be, in the hadoop environment.

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

;; As far as the next few functions are concerned, I believe Tom
;; White, in the wonderful [Hadoop: The Definitive
;; Guide](http://goo.gl/nPWWk), put it best: "Hadoop has a bewildering
;; number of configuration properties".

(defn default-properties
  "Returns a nested map of Hadoop default configuration properties,
  named according to the 0.20 api."
  [name-node-ip job-tracker-ip]
  (let [owner-dir (stevedore/script (user/user-home ~hadoop-user))
        owner-subdir (partial str owner-dir)]
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

;; TODO -- discuss final properties, here.

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

(def-phase-fn config-files
  "Accepts a base directory and a map of [config-filename,
property-map] pairs, and augments the supplied request to allow for
the creation of each referenced configuration file within the base
directory."
  [config-dir properties]
  (for [[filename props] properties]
    (remote-file/remote-file
     (format "%s/%s.xml" config-dir (name filename))
     :content (properties->xml props)
     :owner hadoop-user :group hadoop-group)))

(defn merge-config
  "Merges two hadoop configuration option maps together, with the
  entries in new-props taking precedence."
  [default-props new-props]
  (apply merge
         (for [[name props] default-props]
           {name (merge props (name new-props))})))

;; TODO -- we need the ability to add custom properties, here!
(def-phase-fn env-file
  "Phase that creates the `hadoop-env.sh` file with references to the
  supplied pid and log dirs. To `hadoop-env.sh` will be placed within the supplied config directory."
  [config-dir log-dir pid-dir]
  (remote-file/remote-file
   (str config-dir "/hadoop-env.sh")
   :content (format-exports
             :HADOOP_PID_DIR pid-dir
             :HADOOP_LOG_DIR log-dir
             :HADOOP_SSH_OPTS "\"-o StrictHostKeyChecking=no\""
             :HADOOP_OPTS "\"-Djava.net.preferIPv4Stack=true\"")))

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

;; TODO -- Are we setting references to pid-dir and log-dir in the
;; configuration files? If so, we're going to need to override those
;; defaults with the pid-dir and log-dir described below, and pull
;; them out of the properties map, like we do with tmp-dir.

(def-phase-fn configure
  "Configures a Hadoop cluster by creating all required default
  directories, and populating the proper configuration file
  options. The `properties` parameter must be a map of the form

    {:core-site {:key val...}
     :hdfs-site {:key val ...}
     :mapred-site {:key val ...}}

  No other top-level keys are supported at this time."
  [namenode-tag jobtracker-tag ip-type properties]
  (expose-request-as
   [request]
   (let [conf-dir (str hadoop-home "/conf")
         etc-conf-dir (stevedore/script
                       (str (config-root) "/hadoop"))
         nn-ip (get-master-ip request ip-type namenode-tag)
         jt-ip (get-master-ip request ip-type jobtracker-tag)
         pid-dir (stevedore/script (str (pid-root) "/hadoop"))
         log-dir (stevedore/script (str (log-root) "/hadoop"))
         defaults  (default-properties nn-ip jt-ip)
         properties (merge-config defaults properties)
         tmp-dir (get-in properties [:core-site :hadoop.tmp.dir])]
     (for [path  [conf-dir tmp-dir log-dir pid-dir]]
       (directory/directory path
                            :owner hadoop-user
                            :group hadoop-group
                            :mode "0755"))
     (file/symbolic-link conf-dir etc-conf-dir)
     (config-files conf-dir properties)
     (env-file conf-dir log-dir pid-dir))))

(script/defscript as-user [user & command])
(stevedore/defimpl as-user :default [user & command]
  (su -s "/bin/bash" ~user
      -c "\"" (str "export JAVA_HOME=" (java-home) ";") ~@command "\""))
(stevedore/defimpl as-user [#{:yum}] [user & command]
  ("/sbin/runuser" -s "/bin/bash" - ~user -c ~@command))

(def-phase-fn hadoop-service
  "Run a Hadoop service"
  [hadoop-daemon description]
  (exec-script/exec-checked-script
   (str "Start Hadoop " description)
   (as-user
    ~hadoop-user
    ~(stevedore/script
      (if-not (pipe (jps)
                    (grep "-i" ~hadoop-daemon))
        ((str ~hadoop-home "/bin/hadoop-daemon.sh")
         "start"
         ~hadoop-daemon))))))

(def-phase-fn hadoop-command
  "Runs '$ hadoop ...' on each machine in the request. Command runs
  has the hadoop user."
  [& args]
  (exec-script/exec-checked-script
   (apply str "hadoop " (interpose " " args))
   (as-user
    ~hadoop-user
    (str ~hadoop-home "/bin/hadoop")
    ~@args)))

(def-phase-fn format-hdfs
  "Formats HDFS for the first time. If HDFS has already been
  formatted, does nothing."
  []
  (exec-script/exec-script
   (as-user ~hadoop-user
            (pipe
             (echo "N")
             ((str ~hadoop-home "/bin/hadoop")
              "namenode"
              "-format")))))

;; TODO -- think about the dfsadmin call, etc, that's happening
;; here. is that needed? Do we need to run this mkdir?
(def-phase-fn name-node
  "Collection of all subphases required for a namenode."
  [data-dir]
  format-hdfs
  (hadoop-service "namenode" "Name Node")
  (hadoop-command "dfsadmin" "-safemode" "wait")
  (hadoop-command "fs" "-mkdir" data-dir)
  (hadoop-command "fs" "-chmod" "+w" data-dir))

(def-phase-fn secondary-name-node []
  (hadoop-service "secondarynamenode" "secondary name node"))

(def-phase-fn job-tracker []
  (hadoop-service "jobtracker" "job tracker"))

(def-phase-fn data-node []
  (hadoop-service "datanode" "data node"))

(def-phase-fn task-tracker []
  (hadoop-service "tasktracker" "task tracker"))