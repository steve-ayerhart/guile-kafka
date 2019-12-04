(define-module (kafka protocol api)
  #:use-module (kafka protocol messages)
  #:use-module (kafka protocol decode)
  #:use-module (kafka protocol encode)

  #:use-module (ice-9 binary-ports)
  #:use-module (rnrs bytevectors)
  #:export (request-api-version))


(define (request-api-version sock)
  (parameterize ((current-api-version 2))
    (define encoded-request
      (encode-request (api-version-request-schema)
                      `(18 ,(current-api-version) 1337 "guile")))
    (put-bytevector sock encoded-request)
    (force-output sock)
    (let* ((size (bytevector-s32-ref (get-bytevector-n sock 4) 0 'big))
           (encoded-response (get-bytevector-n sock size)))
      (decode-request (api-version-response-schema) encoded-response))))
