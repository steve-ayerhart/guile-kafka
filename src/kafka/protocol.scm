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

(define (decode-type type bv index)
  (match type
    ('int16
     (values (bytevector-s16-ref bv index (endianness big)) (+ 2 index)))
    ('int32
     (values (bytevector-s32-ref bv index (endianness big)) (+ 4 index)))
    ('string
     (let* ((str-len (bytevector-s16-ref bv index (endianness big)))
            (str-bv (make-bytevector str-len)))
       (bytevector-copy! bv (+ 2 index) str-bv 0 str-len)
       (values (utf8->string str-bv) (+ str-len index))))))

(define fake-schema
  `((topics ((topic . string) (poop . string)))))


(define (decode-schema schema bv)
  (let decode ((schema schema)
               (index 0)
               (decoded-values '()))
    (format #t "~a\n" schema)
    (format #t "values: ~a\n" decoded-values)
    (if (null? schema)
        (values decoded-values index)
        (match (car schema)
;          ((key (type))
;           (format #t "single array: ~a\n" (car schema))
;           (let array-loop ((count (bytevector-s32-ref bv index (endianness big)))
;                            (index (+ 4 index))
;                            (array-values '()))
;             (format #t "COUNT: ~a\n" count)
;             (if (= 0 count)
;                 (decode (cdr schema)
;                         index
;                         (cons (cons (reverse array-values) '()) decoded-values)
;                 (receive (vals new-index)
;                     (decode type index array-values)
;                   (array-loop (- count 1)
;                               new-index
;                               vals)
          ((key . 'int16)
           (format #t "16\n")
           (decode (cdr schema)
                   (+ 2 index)
                   (acons key (bytevector-s16-ref bv index (endianness big)) decoded-values)))
          ((key . 'int32)
           (format #t "32\n")
           (decode (cdr schema)
                   (+ 4 index)
                   (acons key (bytevector-s32-ref bv index (endianness big)) decoded-values)))
          ((key . 'string)
           (format #t "string\n")
           (let* ((str-len (bytevector-s16-ref bv index (endianness big)))
                  (str-bv (make-bytevector str-len)))
             (bytevector-copy! bv (+ 2 index) str-bv 0 str-len)
             (decode (cdr schema)
                     (+ 2 str-len index)
                     (acons key (utf8->string str-bv) decoded-values))))
;          ((array-key ((key . value) ..1))
;           (error "FART"))
          ((key types)
           (format #t "array!\n")
           (let array-loop ((count (bytevector-s32-ref bv index (endianness big)))
                            (index (+ 4 index))
                            (array-values '()))
             (format #t "COUNT: ~a\n" count)
             (if (= 0 count)
                 (decode (cdr schema)
                         index
                         (acons key (cons (reverse array-values) '()) decoded-values))
                 (receive (vals new-index)
                     (decode types index array-values)
                   (array-loop (- count 1)
                               new-index
                               vals)))))
          ((key types)
           (format #t "array!\n")
           (let array-loop ((count (bytevector-s32-ref bv index (endianness big)))
                            (index (+ 4 index))
                            (array-values '()))
             (format #t "COUNT: ~a\n" count)
             (if (= 0 count)
                 (decode (cdr schema)
                         index
                         (acons key (cons (reverse array-values) '()) decoded-values))
                 (receive (vals new-index)
                     (decode types index array-values)
                   (array-loop (- count 1)
                               new-index
                               vals)))))
          ((key types)
           (format #t "array!\n")
           (let array-loop ((count (bytevector-s32-ref bv index (endianness big)))
                            (index (+ 4 index))
                            (array-values '()))
             (format #t "COUNT: ~a\n" count)
             (if (= 0 count)
                 (decode (cdr schema)
                         index
                         (acons key (cons (reverse array-values) '()) decoded-values))
                 (receive (vals new-index)
                     (decode types index array-values)
                   (array-loop (- count 1)
                               new-index
                               vals)))))))))

(define (encode-type type value)
  (match type
    ('boolean
     (make-bytevector 1 (if value 1 0)))
    ('int16
     (let ((bv (make-bytevector 2)))
       (bytevector-s16-set! bv 0 value 'big)
       bv))
    ('int32
     (let ((bv (make-bytevector 4)))
       (bytevector-s32-set! bv 0 value 'big)
       bv))
    ('string
     (if (string-null? value)
         (let ((bv (make-bytevector 2)))
           (bytevector-s16-set! bv 0 -1 'big)
           bv)
         (let* ((bv-string (string->utf8 value))
                (bv-string-length (bytevector-length bv-string))
                (bv (make-bytevector (+ 2 bv-string-length))))
           (bytevector-s16-set! bv 0 bv-string-length 'big)
           (bytevector-copy! bv-string 0 bv 2 bv-string-length)
           bv)))))

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
  ((define encoded-val (make-bytevector 2))
   (bytevector-s16-set! encoded-val 0 val 'big)
   encoded-val))

(define (encode-int32 val)
  ((define encoded-val (make-bytevector 4))
   (bytevector-s32-set! encoded-val 0 val 'big)
   encoded-val))

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
                ('boolean
                 (put-bytevector bv-port (encode-boolean val))
                 (encode (cdr-schema) (cdr vals)))
                ('int16 (encode-int16))
                ('int32 (encode-int32))
                ('string (encode-string val)))))))))


(define (encode-array schema values)
  (call-with-values
      (λ ()
        (open-bytevector-output-port))
    (λ (bv-port get-bytevector)
      (let ((type (car schema)))
        (if (pair? type)
            (format #t "not yet\n")
            (let ((values-length (length values)))
              (for-each (λ (value)
                          (put-bytevector bv-port (encode-type type value)))
                        values)
              (let* ((bv-values (get-bytevector))
                     (bv-values-length (bytevector-length bv-values))
                     (bv-array (make-bytevector (+ 4 bv-values-length))))
                (bytevector-s32-set! bv-array 0 values-length (endianness big))
                (bytevector-copy! bv-values 0 bv-array 4 bv-values-length)
                bv-array)))))))

;(define (encode-schema types values)
;  (call-with-values
;      (λ ()
;        (open-bytevector-output-port)))
;    (λ (bv-port get-bytevector)
;      (for-each (λ (type value)
;                  (put-bytevector bv-port (if (list? type) (encode-array type value) (encode-type type value)))
;                types
;                values))))
;      (let* ((bv-values (get-bytevector))
;             (bv-values-length (bytevector-length bv-values))
;             (bv-schema (make-bytevector (+ 4 bv-values-length)))))
;        (bytevector-s32-set! bv-schema 0 bv-values-length (endianness big))
;        (bytevector-copy! bv-values 0 bv-schema 4 bv-values-length)
;        bv-schema))))

(define (encode-metadata-request header-data topics)
  (define metadata-request-type-schema `(,@message-header-type-schema (string)))
  (encode-schema metadata-request-type-schema `(,@header-data ,topics)))

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

