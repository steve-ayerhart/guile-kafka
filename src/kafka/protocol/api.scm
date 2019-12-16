(define-module (kafka protocol api)
  #:use-module (kafka protocol messages)
  #:use-module (kafka protocol decode)
  #:use-module (kafka protocol encode)

  #:use-module (ice-9 optargs)
  #:use-module (ice-9 binary-ports)
  #:use-module (srfi srfi-1)
  #:use-module (rnrs bytevectors)
  #:export (request-api-version
            request-sasl-handshake
            request-sasl-authenticate))

;;; TODO add some syntax for defining API keys
;;; TODO abstract out response boilerplate
;;; TODO figure out a way to pass header in a sane way

(define api-versions (make-parameter #f))

(define error-codes
  '((-1 . (unknown-server-error . #f))
    (0  . (none . #f))
    (1  . (offset-out-of-range . #f))
    (2  . (corrupt-message . #t))
    (3  . (unknown-topic-or-partition . #t))
    (4  . (invalid-fetch-size . #f))
    (5  . (leader-not-available . #t))
    (6  . (not-leader-for-partition . #t))
    (7  . (request-timed-out . #t))
    (8  . (broker-not-available . #f))
    (9  . (replica-not-available . #f))
    (10 . (message-too-large . #f))
    (11 . (stale-controller-epock . #f))
    (12 . (offset-metadata-too-large . #f))
    (13 . (network-exception . #t))
    (14 . (coordinator-load-in-progress . #t))
    (15 . (coordinator-not-available . #t))
    (16 . (not-coordinator . #t))
    (17 . (invalid-topic-exception . #f))
    (18 . (record-list-too-large . #f))
    (19 . (not-enough-replicas . #t))
    (20 . (not-enough-replicas-after-append . #t))
    (21 . (invalid-required-acks . #f))
    (22 . (illegal-generation . #f))
    (23 . (inconsistent-group-protocol . #f))
    (24 . (invalid-group-id . #f))
    (25 . (unknown-member-id . #f))
    (26 . (invalid-session-timeout . #f))
    (27 . (rebalance-in-progress . #f))
    (28 . (invalid-commit-offset-size . #f))
    (29 . (topic-authorization-failed . #f))
    (30 . (group-authorization-failed . #f))
    (31 . (cluster-authorization-failed . #f))
    (32 . (invalid-timestamp . #f))
    (33 . (unsupported-sasl-mechanism . #f))
    (34 . (illegal-sasl-state . #f))
    (35 . (unsupported-version . #f))
    (36 . (topic-already-exists . #f))
    (37 . (invalid-partitions . #f))
    (38 . (invalid-replication-factor . #f))
    (39 . (invalid-replica-assignment . #f))
    (40 . (invalid-config . #f))
    (41 . (not-controller . #t))
    (42 . (invalid-request . #f))
    (43 . (unsupported-for-message-format . #f))
    (44 . (policy-violation . #f))
    (45 . (out-of-order-sequence . #f))
    (46 . (duplicate-sequence-number . #f))
    (47 . (invalid-producer-epoch . #f))
    (48 . (invalid-txn-state . #f))
    (49 . (invalid-producer-id-map . #f))
    (50 . (invalid-transation-timeout . #f))
    (51 . (concurrent-transactions . #f))
    (52 . (transaction-coordinator-fenced . #f))
    (53 . (transactional-id-authorization-failed . #f))
    (54 . (security-disabled . #f))
    (55 . (operation-not-attempted . #f))
    (56 . (kafka-storage-error . #t))
    (57 . (log-dir-not-found . #f))
    (58 . (sasl-authentication-failed . #f))
    (59 . (unknown-producer-id . #f))
    (60 . (reassignment-in-progress . #f))
    (61 . (delegation-token-auth-disabled . #f))
    (62 . (delegation-token-not-found . #f))
    (63 . (delegation-token-owner-mismatch . #f))
    (64 . (delegation-token-request-not-allowed . #f))
    (65 . (delegation-token-authorization-failed . #f))
    (66 . (delegation-token-expired . #f))
    (67 . (invalid-principal-type . #f))
    (68 . (non-empty-group . #f))
    (69 . (group-id-not-found . #f))
    (70 . (fetch-session-id-not-found . #t))
    (71 . (invalid-fetch-session-epoch . #t))
    (72 . (listener-not-found . #t))
    (73 . (topic-deletion-disabled . #f))
    (74 . (fenced-leader-epoch . #t))
    (75 . (unknown-leader-epoch . #t))
    (76 . (unsupported-compression-type . #f))
    (77 . (stale-broker-epoch . #f))
    (78 . (offset-not-available . #t))
    (79 . (member-id-required . #f))
    (80 . (preferred-leader-not-available . #t))
    (81 . (group-max-size-reached . #f))
    (82 . (fenced-instance-id . #f))))
(define* (highest-supported-api-version api-key #:rest versions)
  0)

(define req-header-schema
  '(sint16 sint16 sint32 string))

(define-syntax define-kafka-api
  (λ (stx)
    (syntax-case stx ()
      ((_ name api-key request-schemas response-schemas ((api-version ...) (formal ...)) ...)
       #'(define name
           (let ((key api-key) (req-schemas request-schemas) (resp-schemas response-schemas))
             (case-lambda*
              ((sock formal ... #:optional (correlation-id 1337) (client-id "guile-kafka"))
               (let* ((current-api-version (highest-supported-api-version api-key api-version ...))
                      (current-schema `(,req-header-schema ,@(assoc-ref req-schemas current-api-version)))
                      (current-vals (list key current-api-version correlation-id client-id formal ...))
                      (encoded-request (encode-request current-schema current-vals)))

                 (put-bytevector sock encoded-request)
                 (force-output sock)

                 (let* ((size (bytevector-s32-ref (get-bytevector-n sock 4) 0 (endianness big)))
                        (encoded-response (get-bytevector-n sock size)))
                   (decode-response (assoc-ref resp-schemas current-api-version) encoded-response))))
              ...)))))))


(define metadata-request-schemas
  '((0 ((string)))
    (1 ((string)))
    (2 ((string)))
    (3 ((string)))
    (4 ((string) boolean))
    (5 ((string) boolean))
    (6 ((string) boolean))
    (7 ((string) boolean))
    (8 ((string) boolean boolean boolean))))

(define metadata-response-schemas
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

(define-kafka-api
  request-metadata 3
  metadata-request-schemas
  metadata-response-schemas
  ((3 2 1 0)
   (topics))
  ((7 6 5 4)
   (topics allow-auto-topic-creation))
  ((8)
   (topics allow-auto-topic-creation include-cluster-authorized-operations include-topic-authorized-operations)))
;; TODO handle errors
(define* (request-api-versions sock #:optional (correlation-id 1337) (client-id "guile-kafka"))
  (define api-versions-response-schema
    '((correlation-id . sint32)
      (error-code . sint16)
      (api-versions ((api-key . sint16) (min-version . sint16) (max-version . sint16)))
      (throttle-time-ms . sint32)))

  (define encoded-request (encode-request message-header-type-schema `(18 2 ,correlation-id ,client-id)))

  (put-bytevector sock encoded-request)
  (force-output sock)

  (let* ((size (bytevector-s32-ref (get-bytevector-n sock 4) 0 'big))
         (encoded-response (get-bytevector-n sock size)))
    (api-versions
     (map (λ (api-version)
            (cons (cdar api-version) (cdr api-version)))
          (car (assoc-ref (decode-response api-versions-response-schema encoded-response) 'api-versions))))))

(define (request-sasl-handshake sock mechanism)
  ;; TODO version 1 wraps sasl-authenticate with kafka headers, 0 does not
  ;;      handle ths gracefully
  (parameterize ((current-api-version 1))
    (define encoded-request
      (encode-request (sasl-handshake-request-schema)
                      `(17 ,(current-api-version) 1337 "guile" ,mechanism)))
    (put-bytevector sock encoded-request)
    (force-output sock)
    (let* ((size (bytevector-s32-ref (get-bytevector-n sock 4) 0 'big))
           (encoded-response (get-bytevector-n sock size)))
      (decode-response (sasl-handshake-response-schema) encoded-response))))

(define (request-sasl-authenticate sock mechanism)
  (define encoded-request
    (encode-request (sasl-authenticate-request-schema)
                    `(36 ,(current-api-version) 1337 "guile" ,mechanism)))
  (put-bytevector sock encoded-request)
  (force-output sock)
  (let* ((size (bytevector-s32-ref (get-bytevector-n sock 4) 0 'big))
         (encoded-response (get-bytevector-n sock size)))
    (decode-response (sasl-authenticate-response-schema) encoded-response)))
