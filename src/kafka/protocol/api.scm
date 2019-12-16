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
              ...))))
      ((_ name api-key request-schemas response-schemas ((api-version ...) (formal ...) body) ...)
       #'(define name
           (let ((key api-key) (req-schemas request-schemas) (resp-schemas response-schemas))
             (case-lambda*
              ((sock formal ... #:optional (correlation-id 1337) (client-id "guile-kafka"))
               (let ((current-api-version (highest-supported-api-version api-key api-version ...)))
                 body))
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
