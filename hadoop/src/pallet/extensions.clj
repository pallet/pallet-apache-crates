(ns pallet.extensions
  (:use [clojure.contrib.def :only (name-with-attributes)])
  (:require pallet.resource.filesystem-layout
            [clojure.contrib.condition :as condition]
            [clojure.contrib.macro-utils :as macro]))

;; ### Pallet Extensions
;;
;; We start with a few extensions to pallet's crate writing
;; facilities. These may or may not make it into pallet proper; we use
;; them here for demonstration, and as an example of the flexibility
;; that a `phase-fn` with arguments might afford.
;;
;; Here's the macro we've been waiting for. Pallet makes heavy use of
;; threading to build up its requests; each phase accepts an argument
;; vector, binds locals, and either directly modify the request, or
;; threads it through more primitive subphases. `-->` layers various
;; flow control constructs onto `->`, allowing for more natural
;; expressions within the body of the thread. For example:
;;
;;    (--> 10
;;         (for [x (range 10)]
;;            (+ x)))
;;    ;=> 55

(defmacro -->
  "Similar to `clojure.core/->`, but includes symbol macros  for `when`,
  `let` and `for` commands on the internal threading
  expressions. Future iterations will include more symbol macro
  bindings."
  [& forms]
  `(macro/symbol-macrolet
    [~'when pallet.thread-expr/when->
     ~'for pallet.thread-expr/for->
     ~'let pallet.thread-expr/let->
     ~'binding pallet.thread-expr/binding->
     ~'expose-request-as pallet.thread-expr/arg->]
    (-> ~@forms)))

;; #### Phase Macros
;;
;; The `-->` macro above opens the door for a more abstract way to
;; write phases. By capturing the pattern of a function that threads
;; its first argument through the rest of its forms, we can simplify
;; phase function definitions, while making their signatures clearer
;; to the user. (`(def-phase-fn some-phase ...)` presents a simpler
;; signal than `(defn some-phase [arg ...] (-> arg ...))`; the first
;; is a crate function, the second may not be.)
;;
;; Additionally, by controlling the way in which request threading
;; occurs, we gain the ability to insert checks between every form in
;; passed in to the phase function. `check-session` is a simple test
;;that makes sure that the session exists, and is a map.

(defn check-session
  "Function that can check a session map to ensure it is a valid part of
   phase definition. It returns the session map.

   On failure, the function will print the phase through which the
   session passed prior to crashing. It is like that this phase
   introduced the fault into the session; to aid in debugging, make
   sure to use `phase-fn` and `def-phase-fn` to build phases."
  [session form]
  (if (and session (map? session))
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

(defmacro phase-fn
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
  ([argvec] (phase-fn argvec identity))
  ([argvec subphase & left]
     `(fn [session# ~@argvec]
        (--> session#
             ~subphase
             (check-session ~(str subphase))
             ~@(when left
                 [`((phase-fn ~argvec ~@left) ~@argvec)])))))

(defmacro def-phase-fn
  "Binds a `phase-fn` to the supplied name."
  [name & rest]
  (let [[name [argvec & body]]
        (name-with-attributes name rest)]
    `(def ~name
       (phase-fn ~argvec ~@body))))

;; `phase` is deprecated by pallet 0.5.0 in favor of `phase-fn` with
;; no argument vector... I do have to say, though, in a world where
;; `phase-fn` DOES have an argument vector, it becomes nice at the top
;; level to be able to compose various phases without that empty
;; argument vector, like so:
;;
;;    (phase
;;      (java/java :jdk)
;;      hadoop/install)
;;
;; rather than
;;
;;    (phase-fn []
;;      (java/java :jdk)
;;      hadoop/install)
;;
;; So, to see how it looks, and for backwards compatibility, we
;; provide `phase`.

(defmacro phase [& forms]
  `(phase-fn [] ~@forms))
