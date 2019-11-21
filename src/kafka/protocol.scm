(define-module (kafka protocol)
  #:use-module (rnrs bytevectors)
  #:use-module (ice-9 match)
  #:use-module (ice-9 optargs)
  #:use-module (ice-9 binary-ports)

  #:export (encode-metadata-request))

(define request-header-schema
  '(int16 int16 int32 string))

(define (encode-metadata-request topics . header-data)
  (define topics-length (length topics))
  (define metadata-request-schema `(,@request-header-schema int32 ,@(make-list topics-length 'string)))
  (define metadata-request-data `(,@header-data ,topics-length ,@topics))
  (encode-request metadata-request-schema metadata-request-data))

(define (encode-request schema data)
  (call-with-values
      (λ ()
        (open-bytevector-output-port))
    (λ (bv-port get-bytevector)
      (for-each (λ (type data)
                  (put-bytevector bv-port (encode-type type data)))
                schema
                data)

      (let* ((data (get-bytevector)) (data-length (bytevector-length data))
             (bv (make-bytevector (+ 4 data-length))))
        (bytevector-s32-set! bv 0 data-length 'big)
        (bytevector-copy! data 0 bv 4 data-length)
        bv))))

(define (encode-type type value)
  (match type
    (boolean
     (make-bytevector 1 (if value 1 0)))
    (int16
     (let ((bv (make-bytevector 2)))
       (bytevector-s16-set! bv 0 value 'big)
       bv))
    (int32
     (let ((bv (make-bytevector 4)))
       (bytevector-s32-set! bv 0 value 'big)
       bv))
    (string
     (if (string-null? value)
         (let ((bv (make-bytevector 2)))
           (bytevector-s16-set! bv 0 -1 'big)
           bv)
         (let* ((bv-string (string->utf8 value))
                (bv-string-length (bytevector-length bv-string))
                (bv (make-bytevector (+ 4 bv-string-length))))
           (bytevector-s16-set! bv 0 bv-string-length 'big)
           (bytevector-copy! bv-string 0 bv 4 bv-string-length)
           bv)))))
