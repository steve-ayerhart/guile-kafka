(define-module (kafka protocol api)
  #:use-module (kafka protocol messages)
  #:use-module (kafka protocol decode)
  #:use-module (kafka protocol encode)

  #:use-module (ice-9 optargs)
  #:use-module (ice-9 receive)
  #:use-module (ice-9 binary-ports)
  #:use-module (srfi srfi-1)
  #:use-module (rnrs bytevectors)
  #:export (api-versions
            request-metadata
            request-api-versions
            request-sasl-handshake
            request-sasl-authenticate))

;;; TODO add some syntax for defining API keys
;;; TODO abstract out response boilerplate
;;; TODO figure out a way to pass header in a sane way

(define api-versions (make-parameter #f))

                                        ;(define-kafka-api
                                        ;  request-metadata 3
                                        ;  metadata-request-schemas
                                        ;  metadata-response-schemas
                                        ;  (0 3 (topics))
                                        ;  (4 7 (topics allow-auto-topic-creation))
                                        ;  (8 8 (topics allow-auto-topic-creation include-cluster-authorized-operations include-topic-authorized-operations)))

(define (highest-supported-api-version api-key)
  (assoc-ref (assoc-ref (api-versions) api-key) 'max-version))

(define (request-api sock api-key api-version request-schema response-schema payload)
  (let* ((current-schema `(,@request-message-header-schema ,@request-schema))
         (encoded-request (encode-request current-schema payload)))

    (put-bytevector sock encoded-request)
    (force-output sock)

    (let* ((size (bytevector-s32-ref (get-bytevector-n sock 4) 0 (endianness big)))
           (encoded-response (get-bytevector-n sock size))
           (decode-schema (acons 'correlation-id 'sint32 response-schema)))
      (decode-response decode-schema encoded-response))))

(define-syntax define-kafka-api
  (λ (stx)
    (syntax-case stx ()
      ((_ name api-key request-schemas response-schemas (formal-min-version formal-max-version (formal ...)) ...)
       #'(define name
           (let* ((key api-key)
                  (req-schemas request-schemas)
                  (resp-schemas response-schemas)
                  (versions (assoc-ref (api-versions) key)))
             (case-lambda*
              ((sock formal ... #:optional (correlation-id 1337) (client-id "guile-kafka"))
               (let* ((current-api-version (assoc-ref versions 'max-version))
                      (current-schema `(,@request-message-header-schema ,@(assoc-ref req-schemas current-api-version)))
                      (current-vals (list key current-api-version correlation-id client-id formal ...))
                      (encoded-request (encode-request current-schema current-vals)))

                 (put-bytevector sock encoded-request)
                 (force-output sock)

                 (let* ((size (bytevector-s32-ref (get-bytevector-n sock 4) 0 (endianness big)))
                        (encoded-response (get-bytevector-n sock size))
                        (decode-schema (acons 'correlation-id 'sint32 (assoc-ref resp-schemas current-api-version))))
                   (decode-response decode-schema encoded-response))))
              ...))))
      ((_ name api-key request-schemas response-schemas (formal-min-version formal-max-version (formal ...) body) ...)
       #'(define name
           (let* ((key api-key)
                 (req-schemas request-schemas)
                 (resp-schemas response-schemas)
                 (versions (assoc-ref (api-versions) key)))
             (case-lambda*
              ((sock formal ... #:optional (correlation-id 1337) (client-id "guile-kafka"))
                 body)
              ...)))))))

(define* (request-sasl-handshake sock mechanism #:optional (correlation-id 1337) (client-id "guile-kafka"))
  (define api-key 17)
  (define api-version 0) ;(highest-supported-api-version api-key))
  (define sasl-handshake-req-schema '(string))
  (define sasl-handshake-resp-schema '((error-code .sint16) (mechanisms . (string))))

  (receive (req-schema resp-schema req-payload)
      (if (= api-version 0)
          (values sasl-handshake-req-schema sasl-handshake-resp-schema (list mechanism))
          (values (append request-message-header-schema sasl-handshake-req-schema)
                  (acons 'correlation-id 'sint32 sasl-handshake-resp-schema)
                  (list api-key api-version correlation-id client-id mechanism)))
    (request-api sock api-key api-version req-schema resp-schema req-payload)))

;(define-kafka-api
;  request-sasl-handshake 17
;  sasl-handshake-request-schemas
;  sasl-handshake-response-schemas
;  (0 1 (mechanism)
;     (let* ((request-schema `(,@(if (= 0 current-api-version) request-message-header-schema '())
;                              ,@(assoc-ref req-schemas current-api-version)))
;            (current-vals (list mechanism))
;            (encoded-request (encode-request request-schema current-vals)))
;
;       (put-bytevector sock encoded-request)
;       (force-output sock)
;
;       (let* ((size (bytevector-s32-ref (get-bytevector-n sock 4) 0 (endianness big)))
;              (encoded-response (get-bytevector-n sock size))
;              (response-schema (if (= 0 current-api-version)
;                                   (assoc-ref resp-schema current-api-version)
;                                   (acons 'correlation-id 'sint32 (assoc-ref resp-schema current-api-version)))))
;         (decode-response response-schema encoded-response)))))


;; TODO handle errors
(define* (request-api-versions sock #:optional (correlation-id 1337) (client-id "guile-kafka"))
  (define api-versions-response-schema
    '((correlation-id . sint32)
      (error-code . sint16)
      (api-versions . ((api-key . sint16) (min-version . sint16) (max-version . sint16)))
      (throttle-time-ms . sint32)))

  (define encoded-request (encode-request request-message-header-schema `(18 2 ,correlation-id ,client-id)))

  (put-bytevector sock encoded-request)
  (force-output sock)

  (let* ((size (bytevector-s32-ref (get-bytevector-n sock 4) 0 'big))
         (encoded-response (get-bytevector-n sock size))
         (decoded-response (decode-response api-versions-response-schema encoded-response)))
    (api-versions
     (map (λ (api-version)
            (cons (cdar api-version) (cdr api-version)))
          (car (assoc-ref decoded-response 'api-versions))))
    decoded-response))

(define-kafka-api
  request-metadata 3
  metadata-request-schemas
  metadata-response-schemas
  (0 3 (topics))
  (4 7 (topics allow-auto-topic-creation))
  (8 8 (topics allow-auto-topic-creation include-cluster-authorized-operations include-topic-authorized-operations)))


                                        ;(define (request-sasl-handshake sock mechanism)
                                        ;  ;; TODO version 1 wraps sasl-authenticate with kafka headers, 0 does not
                                        ;  ;;      handle ths gracefully
                                        ;  (parameterize ((current-api-version 1))
                                        ;    (define encoded-request
                                        ;      (encode-request (sasl-handshake-request-schema)
                                        ;                      `(17 ,(current-api-version) 1337 "guile" ,mechanism)))
                                        ;    (put-bytevector sock encoded-request)
                                        ;    (force-output sock)
                                        ;    (let* ((size (bytevector-s32-ref (get-bytevector-n sock 4) 0 'big))
                                        ;           (encoded-response (get-bytevector-n sock size)))
                                        ;      (decode-response (sasl-handshake-response-schema) encoded-response))))
                                        ;
                                        ;(define (request-sasl-authenticate sock mechanism)
                                        ;  (define encoded-request
                                        ;    (encode-request (sasl-authenticate-request-schema)
                                        ;                    `(36 ,(current-api-version) 1337 "guile" ,mechanism)))
                                        ;  (put-bytevector sock encoded-request)
                                        ;  (force-output sock)
                                        ;  (let* ((size (bytevector-s32-ref (get-bytevector-n sock 4) 0 'big))
                                        ;         (encoded-response (get-bytevector-n sock size)))
                                        ;    (decode-response (sasl-authenticate-response-schema) encoded-response)))
