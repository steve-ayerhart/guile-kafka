(define-module (kafka protocol sasl plain)
  #:use-module (rnrs bytevectors)
  #:use-module (ice-9 optargs)
  #:use-module (gcrypt base64)

  #:export (sasl-plain-client-message))

;;; REFERENCE: https://tools.ietf.org/html/rfc4616

;; TODO: restrict inputs (strictly follow RFC)
;cm9iAHNlY3JldA==

(define* (sasl-plain-client-message authcid passwd #:optional (authzid #f))
  (base64-encode
   (string->utf8
    (string-append (or authzid "") "\x00" authcid "\x00" passwd))))
