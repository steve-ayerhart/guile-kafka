(define-module (kafka protocol api)
  #:use-module (kafka protocol encode)
  #:use-module (kafka protocol decode)

  #:use-module (ice-9 optargs)

  #:use-module (oop goops)

  #:export (current-api-version))

(define current-api-version (make-parameter #f))
