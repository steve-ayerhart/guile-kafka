(define-module (kafka protocol)
  #:use-module (kafka protocol encode)
  #:use-module (kafka protocol decode)

  #:use-module (rnrs bytevectors)
  #:use-module (oop goops)

  #:use-module (srfi srfi-1)
  #:use-module (ice-9 match)
  #:use-module (ice-9 optargs)
  #:use-module (ice-9 receive)
  #:use-module (ice-9 binary-ports))
