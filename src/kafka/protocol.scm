(define-module (kafka protocol)
  #:use-module (rnrs bytevectors)
  #:use-module (oop goops)

  #:use-module (srfi srfi-1)
  #:use-module (ice-9 match)
  #:use-module (ice-9 optargs)
  #:use-module (ice-9 receive)
  #:use-module (ice-9 binary-ports)

  #:export (encode-metadata-request
            decode-metadata-response))

(define message-header-key-schema
  '(api-key api-version correlation-id client-id))
(define message-header-type-schema
  '(int16 int16 int32 string))

(define message-header-request-schema
  (map cons message-header-key-schema message-header-type-schema))

(define message-header-response-schema '((correlation-id . int32)))

(define (decode-int16 encoded-val index)
  (values (bytevector-s16-ref encoded-val index (endianness big))) (+ 2 index))

(define (decode-int32 encoded-val index)
  (values (bytevector-s32-ref encoded-val index (endianness big))) (+ 4 index))

(define (decode-string encoded-val index)
  (define encoded-string-length (bytevector-s16-ref encoded-val index (endianness big)))
  (define encoded-string (make-bytevector encoded-string-length))

  (bytevector-copy! encoded-val (+ 2 index) encoded-string 0 encoded-string-length)

  (values (utf8->string encoded-string) (+ 2 encoded-string-length index)))

(define (decode-array schema encoded-val index)
  (define encoded-array-length (bytevector-s32-ref encoded-val index (endianness big)))
  (let array-loop ((index (+ 2 index))
                   (count encoded-array-length)
                   (array-values '()))
    (if (= 0 count)
        (values (reverse array-values) index)
        (match schema
          (((record ...))
           (values "DO SOMETHIGN"))
          ((single ...)
           (receive (decoded-value new-index)
               (decode-type schema encoded-val)
             (array-loop new-index (- count 1) (cons decoded-value array-values))))))))

(define (decode-type type encoded-val index)
  (match type
    ('int16 (decode-int16 encoded-val index))
    ('int32 (decode-int32 encoded-val index))
    ('string (decode-string encoded-val index))
    ((array-schema) (decode-array array-schema encoded-val index))))

;(define (decode-schema schema bv)
;  (let decode ((schema schema)
;               (index 0)
;               (decoded-values '()))
;    (if (null? schema)
;        decoded-values
;        (match (car schema)
;          ((array ...1)
;           (decode))))

;(define (decode-schema schema bv)
;  (let decode ((schema schema)
;               (index 0)
;               (decoded-values '()))
;    (if (null? schema)
;        (values decoded-values index)
;        (match (car schema)
;          ((key . 'int16)
;           (decode (cdr schema)
;                   (+ 2 index)
;                   (acons key (bytevector-s16-ref bv index (endianness big)) decoded-values)
;          ((key . 'int32)
;           (decode (cdr schema)
;                   (+ 4 index)
;                   (acons key (bytevector-s32-ref bv index (endianness big)) decoded-values)
;          ((key . 'string)
;           (let* ((str-len (bytevector-s16-ref bv index (endianness big)))
;                  (str-bv (make-bytevector str-len))
;             (bytevector-copy! bv (+ 2 index) str-bv 0 str-len)
;             (decode (cdr schema)
;                     (+ 2 str-len index)
;                     (acons key (utf8->string str-bv) decoded-values))
;          ((key types)
;           (let array-loop ((count (bytevector-s32-ref bv index (endianness big)))
;                            (index (+ 4 index))
;                            (array-values '()))
;             (if (= 0 count)
;                 (decode (cdr schema)
;                         index
;                         (acons key (cons (reverse array-values) '()) decoded-values))
;                 (receive (vals new-index)
;                     (decode types index array-values))
;                   (array-loop (- count 1)
;                               new-index
;                               vals)
;          ((key types)
;           (let array-loop ((count (bytevector-s32-ref bv index (endianness big)))
;                            (index (+ 4 index))
;                            (array-values '()))
;             (if (= 0 count)
;                 (decode (cdr schema)
;                         index
;                         (acons key (cons (reverse array-values) '()) decoded-values))
;                 (receive (vals new-index)
;                     (decode types index array-values))
;                   (array-loop (- count 1)
;                               new-index
;                               vals)
;          ((key types)
;           (let array-loop ((count (bytevector-s32-ref bv index (endianness big)))
;                            (index (+ 4 index))
;                            (array-values '()))
;             (if (= 0 count)
;                 (decode (cdr schema)
;                         index
;                         (acons key (cons (reverse array-values) '()) decoded-values))
;                 (receive (vals new-index)
;                     (decode types index array-values))
;                   (array-loop (- count 1)
;                               new-index
;                               vals))))

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

(define (encode-metadata-request header-data topics)
  (define metadata-request-type-schema `(,@message-header-type-schema (string)))
  (encode-request metadata-request-type-schema `(,@header-data ,topics)))

(define (decode-metadata-response response)
  (define broker-schema '((node-id . int32)
                          (host . string)
                          (port . int32)))
  (define partition-schema `((error-code . in16)
                             (partition-index . int32)
                             (leader-id . int32)
                             (replica-nodes (int32))
                             (isr-nodes (int32))))
  (define topic-schema `((error-code . int16)
                         (name . string)
                         (partitions ,partition-schema)))

  (define metadata-response-schema `(,@message-header-response-schema
                                     (brokers ,broker-schema)
                                     (topics ,topic-schema)))
  metadata-response-schema)
;  (decode-schema metadata-response-schema response))
