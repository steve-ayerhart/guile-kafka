(define-module (kafka protocol api)
  #:use-module (kafka protocol messages)
  #:use-module (kafka protocol decode)
  #:use-module (kafka protocol encode)

  #:use-module (ice-9 binary-ports)
  #:use-module (rnrs bytevectors)
  #:export (request-api-version
            request-sasl-handshake
            request-sasl-authenticate))

;;; TODO add some syntax for defining API keys
;;; TODO abstract out response boilerplate
;;; TODO figure out a way to pass header in a sane way

(define (request-api-version sock)
  (parameterize ((current-api-version 2))
    (define encoded-request
      (encode-request (api-version-request-schema)
                      `(18 ,(current-api-version) 1337 "guile"))) ; TODO: not hardcode
    (put-bytevector sock encoded-request)
    (force-output sock)
    (let* ((size (bytevector-s32-ref (get-bytevector-n sock 4) 0 'big))
           (encoded-response (get-bytevector-n sock size)))
      (decode-request (api-version-response-schema) encoded-response))))

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
      (decode-request (sasl-handshake-response-schema) encoded-response))))

(define (request-sasl-authenticate sock mechanism)
  (define encoded-request
    (encode-request (sasl-authenticate-request-schema)
                    `(36 ,(current-api-version) 1337 "guile" ,mechanism)))
  (put-bytevector sock encoded-request)
  (force-output sock)
  (let* ((size (bytevector-s32-ref (get-bytevector-n sock 4) 0 'big))
         (encoded-response (get-bytevector-n sock size)))
    (decode-request (sasl-authenticate-response-schema) encoded-response)))
