(define-module (kafka protocol messages)
  #:use-module (ice-9 regex)
  #:export (current-api-version
            api-version-request-schema
            api-version-response-schema
            metadata-request-schema
            metadata-response-schema
            sasl-authenticate-request-schema
            sasl-authenticate-response-schema))

(define current-api-version (make-parameter 0))

(define message-header-type-schema
  '(sint16 sint16 sint32 string))

;;; TODO is there a better way to define api schemas?

(define-syntax-rule (define-schema name schemas)
  "defines a function @name that returns a schema based of the
@current-api-version parameter"
  (define (name)
    (let fetch ((version (current-api-version)))
      (let ((schema (assoc-ref schemas version)))
        (if (or schema (= 0 version))
            (if (regexp-match? (string-match "request" (symbol->string (procedure-name name))))
                (append message-header-type-schema schema)
                (acons 'correlation-id 'sint32 (car schema)))
            (fetch (- version 1)))))))

(define-schema api-version-request-schema '((0) (1) (2)))
(define-schema api-version-response-schema
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

(define-schema metadata-request-schema
  '((0 ((string)))
    (1 ((string)))
    (2 ((string)))
    (3 ((string)))
    (4 ((string) boolean))
    (5 ((string) boolean))
    (6 ((string) boolean))
    (7 ((string) boolean))
    (8 ((string) boolean boolean boolean))))

(define-schema metadata-response-schema
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

(define-schema sasl-authenticate-request-schema
  '((0 (bytes))
    (1 (bytes))))

(define-schema sasl-authenticate-response-schema
  '((0 ((error-code . sint16)
        (error-message . nullable-string)
        (auth-bytes . bytes)))
    (1 ((error-code . sint16)
        (error-message . nullable-string)
        (auth-bytes . bytes)
        (session-lifetime-ms . sint64)))))
