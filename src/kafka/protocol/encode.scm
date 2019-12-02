(define-module (kafka protocol decode)
  #:use-module (rnrs bytevectors)
  #:use-module (ice-9 match)
  #:use-module (ice-9 receive))

(define (encode-string str)
  (define utf8-string (string->utf8 str))
  (define utf8-string-length (bytevector-length utf8-string))
  (define encoded-string (make-bytevector (+ 2 utf8-string-length)))
  (bytevector-s16-set! encoded-string 0 utf8-string-length (endianness big))
  (bytevector-copy! utf8-string 0 encoded-string 2 utf8-string-length)
  encoded-string)

(define (encode-boolean bool)
  (make-bytevector 1 (if bool 1 0)))

(define (encode-int16 val)
  (define encoded-val (make-bytevector 2))
  (bytevector-s16-set! encoded-val 0 val 'big)
  encoded-val)

(define (encode-int32 val)
  (define encoded-val (make-bytevector 4))
  (bytevector-s32-set! encoded-val 0 val 'big)
  encoded-val)

(define (encode-array schema vals)
  (define value-array-length (length vals))
  (call-with-values
      (λ ()
        (open-bytevector-output-port))
    (λ (bv-port get-bytevector)
      (let array-loop ((vals vals))
        (if (null? vals)
            (let* ((values-bv (get-bytevector))
                   (values-bv-length (bytevector-length values-bv))
                   (array-bv (make-bytevector (+ 4 values-bv-length))))
              (bytevector-s32-set! array-bv 0 value-array-length (endianness big))
              (bytevector-copy! values-bv 0 array-bv 4 values-bv-length)
              array-bv)
            (match schema
              (((record ..1))
               (put-bytevector bv-port (encode-schema record (car vals)))
               (array-loop (cdr vals)))
              ((single)
               (put-bytevector bv-port (encode-schema schema vals))
               (array-loop (cdr vals)))))))))

(define (encode-schema schema vals)
  (call-with-values
      (λ ()
        (open-bytevector-output-port))
    (λ (bv-port get-bytevector)
      (let encode ((schema schema)
                   (vals vals))
        (if (null? schema)
            (get-bytevector)
            (let ((type (car schema))
                  (val (car vals)))
              (match type
                ((array-schema ..1)
                 (put-bytevector bv-port (encode-array array-schema val))
                 (encode (cdr schema) (cdr vals)))
                ('boolean
                 (put-bytevector bv-port (encode-boolean val))
                 (encode (cdr schema) (cdr vals)))
                ('int16
                 (put-bytevector bv-port (encode-int16 val))
                 (encode (cdr schema) (cdr vals)))
                ('int32
                 (put-bytevector bv-port (encode-int32 val)) (encode (cdr schema) (cdr vals)))
                ('string
                 (put-bytevector bv-port (encode-string val))
                 (encode (cdr schema) (cdr vals))))))))))

(define (encode-request request-schema vals)
  (define encoded-data (encode-schema request-schema vals))
  (define encoded-data-length (bytevector-length encoded-data))
  (define encoded-request (make-bytevector (+ 4 encoded-data-length)))
  (bytevector-s32-set! encoded-request 0 encoded-data-length (endianness big))
  (bytevector-copy! encoded-data 0 encoded-request 4 encoded-data-length)
  encoded-request)

(define message-header-type-schema
  '(int16 int16 int32 string))

(define (encode-metadata-request header-data topics)
  (define metadata-request-type-schema `(,@message-header-type-schema (string)))
  (encode-request metadata-request-type-schema `(,@header-data ,topics)))
