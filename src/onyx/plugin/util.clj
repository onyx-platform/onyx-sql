(ns onyx.plugin.util)

(def uuid-length 16)

(defn uuid-to-bytes
  [uuid]
  (let [bytes (.putLong (.putLong (java.nio.ByteBuffer/allocate uuid-length)
                                  (.getMostSignificantBits uuid))
                        (.getLeastSignificantBits uuid))
		    array (.array bytes)]
    array))

(defn bytes-to-bigint
  [bytes]
  ; Create ByteBuffer with UUID bytes
  ; Prepend it with 0 to make sure that result bigint is positive
  (let [bytes (.put (.put (java.nio.ByteBuffer/allocate (+ uuid-length
                                                           1))
                          (byte 0))
                    bytes)
        value (bigint (new java.math.BigInteger (.array bytes)))]
    value))

(defn- trim-byte-buffer
  [byte-buffer]
  (.get byte-buffer)
  byte-buffer)

(defn- fill-byte-buffer
  [byte-buffer]
  (let [new-byte-buffer (java.nio.ByteBuffer/allocate (+ 1 (.limit byte-buffer)))
        new-byte-buffer (.put new-byte-buffer (byte 0))
        new-byte-buffer (.put new-byte-buffer (.array byte-buffer))]
  (.rewind new-byte-buffer)
  new-byte-buffer))

(defn- normalize-byte-buffer
  [byte-buffer]
    (cond
      (< uuid-length (.remaining byte-buffer)) (normalize-byte-buffer (trim-byte-buffer byte-buffer))
      (> uuid-length (.remaining byte-buffer)) (normalize-byte-buffer (fill-byte-buffer byte-buffer))
      (= uuid-length ) byte-buffer))

(defn bigint-to-bytes
  [value]
  (let [bytes (.toByteArray (.toBigInteger value))
        byte-buffer (java.nio.ByteBuffer/wrap bytes)
        byte-buffer (normalize-byte-buffer byte-buffer)
        bytes (byte-array uuid-length)]
    (.get byte-buffer bytes)
    bytes))