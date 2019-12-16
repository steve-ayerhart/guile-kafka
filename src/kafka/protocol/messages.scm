(define-module (kafka protocol messages)
  #:use-module (ice-9 regex)
  #:export (request-message-header-schema
            api-versions-request-schema
            api-versions-response-schema
            metadata-request-schema
            metadata-response-schema
            sasl-handshake-request-schema
            sasl-handshake-response-schema
            sasl-authenticate-request-schema
            sasl-authenticate-response-schema))

(define request-message-header-schema
  '(sint16 sint16 sint32 string))

(define api-versions-request-schema '((0 ()) (1 ()) (2 ())))
(define api-versions-response-schema
  '((0 ((error-code . sint16)
        (api-versions ((api-key . sint16)
                       (min-version . sint16)
                       (max-version . sint16)))))
    (1 ((error-code . sint16)
        (api-versions ((api-key . sint16)
                       (min-version . sint16)
                       (max-version . sint16)))))
    (2 ((error-code . sint16)
        (api-versions ((api-key . sint16)
                       (min-version . sint16)
                       (max-version . sint16)))
        (throttle-time-ms . sint32)))))

(define sasl-handshake-request-schema
  '((0 (string))
    (1 (string))))
(define sasl-handshake-response-schema
  '((0 ((error-code . sint16)
        (mechanisms (string))))
    (1 ((error-code . sint16)
        (mechanisms (string))))))

(define metadata-request-schema
  '((0 ((string)))
    (1 ((string)))
    (2 ((string)))
    (3 ((string)))
    (4 ((string) boolean))
    (5 ((string) boolean))
    (6 ((string) boolean))
    (7 ((string) boolean))
    (8 ((string) boolean boolean boolean))))

(define metadata-response-schema
  '((0 ((brokers ((node-id . sint32)
                  (host . string)
                  (port . sint32)))
        (topics ((error-code . sint16)
                 (name . string)
                 (partitions ((error-code . sint16)
                              (partition-index . sint32)
                              (leader-id . sint32)
                              (replica-nodes (sint32))
                              (isr-nodes (sint32))))))))
    (1 ((brokers ((node-id . sint32)
                  (host . string)
                  (port . sint32)
                  (rack . nullable-string)))
        (topics ((error-code . sint16)
                 (name . string)
                 (is-internal . boolean)
                 (partitions ((error-code . sint16)
                              (partition-index . sint32)
                              (leader-id . sint32)
                              (replica-nodes (sint32))
                              (isr-nodes (sint32))))))))
    (2 ((brokers ((node-id . sint32)
                  (host . string)
                  (port . sint32)
                  (rack . nullable-string)))
        (cluster-id . nullable-string)
        (controller-id . sint32)
        (topics ((error-code . sint16)
                 (name . string)
                 (is-internal . boolean)
                 (partitions ((error-code . sint16)
                              (partition-index . sint32)
                              (leader-id . sint32)
                              (replica-nodes (sint32))
                              (,reisr-nodes (sint32))))))))
    (3 ((throttle-time-ms . int32)
        (brokers ((node-id . sint32)
                  (host . string)
                  (port . sint32)
                  (rack . nullable-string)))
        (cluster-id . nullable-string)
        (controller-id . sint32)
        (topics ((error-code . sint16)
                 (name . string)
                 (is-internal . boolean)
                 (partitions ((error-code . sint16)
                              (partition-index . sint32)
                              (leader-id . sint32)
                              (replica-nodes (sint32))
                              (isr-nodes (sint32))))))))))

(define sasl-authenticate-request-schema
  '((0 (bytes))
    (1 (bytes))))

(define sasl-authenticate-response-schema
  '((0 ((error-code . sint16)
        (error-message . nullable-string)
        (auth-bytes . bytes)))
    (1 ((error-code . sint16)
        (error-message . nullable-string)
        (auth-bytes . bytes)
        (session-lifetime-ms . sint64)))))
