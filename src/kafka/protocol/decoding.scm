(define-module (kafka protocol decoding)
  #:use-module (rnrs bytevectors)
  #:use-module (ice-9 match)
  #:use-module (ice-9 receive)

  #:export (decode-metadata-response))

(define (decode-int16 encoded-val index)
  (values (bytevector-s16-ref encoded-val index (endianness big)) (+ 2 index)))

(define (decode-int32 encoded-val index)
  (values (bytevector-s32-ref encoded-val index (endianness big)) (+ 4 index)))

(define (decode-string encoded-val index)
  (define encoded-string-length (bytevector-s16-ref encoded-val index (endianness big)))
  (define encoded-string (make-bytevector encoded-string-length))

  (bytevector-copy! encoded-val (+ 2 index) encoded-string 0 encoded-string-length)

  (values (utf8->string encoded-string) (+ 2 encoded-string-length index)))

(define (decode-array schema encoded-val index)
  (define encoded-array-length (bytevector-s32-ref encoded-val index (endianness big)))
  (let array-loop ((index (+ 4 index))
                   (count encoded-array-length)
                   (array-values '()))
    (if (= 0 count)
        (values (cons (reverse array-values) '()) index)
        (match schema
          (((record ...))
           (receive (decoded-value new-index)
               (decode-schema (car schema) encoded-val index)
             (array-loop new-index (- count 1) (cons decoded-value array-values))))
          ((single ...)
           (receive (decoded-value new-index)
               (decode-type (car single) encoded-val index)
             (array-loop new-index (- count 1) (cons decoded-value array-values))))))))

(define (decode-type type encoded-val index)
  (match type
    ('int16 (decode-int16 encoded-val index))
    ('int32 (decode-int32 encoded-val index))
    ('string (decode-string encoded-val index))
    ((array-schema ...)
     (decode-array array-schema encoded-val index))))

(define (decode-schema schema bv index)
  (let decode ((schema schema)
               (index index)
               (decoded-values '()))
    (if (null? schema)
        (values (reverse decoded-values) index)
        (receive (decoded-value new-index)
            (decode-type (cdar schema) bv index)
          (decode (cdr schema) new-index (acons (caar schema) decoded-value decoded-values))))))

(define message-header-response-schema '((correlation-id . int32)))

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
