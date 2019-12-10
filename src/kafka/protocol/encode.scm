(define-module (kafka protocol encode)
  #:use-module (rnrs bytevectors)
  #:use-module (ice-9 match)
  #:use-module (ice-9 receive)
  #:use-module (ice-9 binary-ports)

  #:export (encode-request encode-schema))

(define (encode-boolean bool)
  (make-bytevector 1 (if bool 1 0)))

(define (encode-sint8 val)
  (make-bytevector 1 val))

(define (encode-sint16 val)
  (define encoded-val (make-bytevector 2))
  (bytevector-s16-set! encoded-val 0 val (endianness big))
  encoded-val)

(define (encode-sint32 val)
  (define encoded-val (make-bytevector 4))
  (bytevector-s32-set! encoded-val 0 val (endianness big))
  encoded-val)

(define (encode-sint64 val)
  (define encoded-val (make-bytevector 8))
  (bytevector-s64-set! encoded-val 0 val (endianness big))
  encoded-val)

(define (encode-uint32 val)
  (define encoded-val (make-bytevector 4))
  (bytevector-u32-set! encoded-val 0 val (endianness big))
  encoded-val)

(define (encode-bytes val)
  (define val-length (bytevector-length val))
  (define encoded-val (make-bytevector val-length))
  (bytevector-s32-set! encoded-val 0 val-length (endianness big))
  (bytevector-copy! val 0 encoded-val 4 val-length)
  encoded-bytes)

(define (encode-nullable-bytes val)
  (if (or (and (boolean? val) (not (bytevector? val))) (= 0 (bytevector-length val)))
      (let ((encoded-bytes (make-bytevector 4)))
        (bytevector-s32-set! encoded-bytes 0 -1 (endianness big))
        encoded-bytes)
      (encode-bytes val)))

;;; TODO: VARINT

;;; TODO: VARLONG

(define (encode-string str)
  (define utf8-string (string->utf8 str))
  (define utf8-string-length (bytevector-length utf8-string))
  (define encoded-string (make-bytevector (+ 2 utf8-string-length)))
  (bytevector-s16-set! encoded-string 0 utf8-string-length (endianness big))
  (bytevector-copy! utf8-string 0 encoded-string 2 utf8-string-length)
  encoded-string)

(define (encode-nullable-string str)
  (if (or (and (boolean? str) (not str)) (string-null? str))
      (let ((encoded-string (make-bytevector 2)))
        (bytevector-s16-set! encoded-string 0 -1 (endianness big))
        encoded-string)
      (encode-string str)))

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
            (if (= 1 (length schema))
                (begin
                  (put-bytevector bv-port (encode-schema schema vals))
                  (array-loop (cdr vals)))
                (begin
                  (put-bytevector bv-port (encode-schema schema (car vals)))
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
                ;;; TODO: add rest of types
                ((array-schema ...)
                 (put-bytevector bv-port (encode-array array-schema val))
                 (encode (cdr schema) (cdr vals)))
                ('boolean
                 (put-bytevector bv-port (encode-boolean val))
                 (encode (cdr schema) (cdr vals)))
                ('sint16
                 (put-bytevector bv-port (encode-sint16 val))
                 (encode (cdr schema) (cdr vals)))
                ('sint32
                 (put-bytevector bv-port (encode-sint32 val)) (encode (cdr schema) (cdr vals)))
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
