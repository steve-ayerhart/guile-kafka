(define-module (kafka protocol decode)
  #:use-module (rnrs bytevectors)
  #:use-module (ice-9 match)
  #:use-module (ice-9 receive)

  #:export (decode-request))

(define (decode-boolean encoded-val index)
  (values (> 0 (bytevector-s8-ref encoded-val index)) (+ 1 index)))

(define (decode-sint8 encoded-val index)
  (values (bytevector-s8-ref encoded-val index) (+ 1 index)))

(define (decode-sint16 encoded-val index)
  (values (bytevector-s16-ref encoded-val index (endianness big)) (+ 2 index)))

(define (decode-sint32 encoded-val index)
  (values (bytevector-s32-ref encoded-val index (endianness big)) (+ 4 index)))

(define (decode-sint64 encoded-val index)
  (values (bytevector-s64-ref encoded-val index (endianness big)) (+ 8 index)))

(define (decode-uint32 encoded-val index)
  (values (bytevector-u32-ref encoded-val index (endianness big)) (+ 4 index)))

;;; TODO: VARINT

;;; TODO: VARLONG

(define (decode-string encoded-val index)
  (define encoded-string-length (bytevector-s16-ref encoded-val index (endianness big)))
  (define encoded-string (make-bytevector encoded-string-length))

  (bytevector-copy! encoded-val (+ 2 index) encoded-string 0 encoded-string-length)

  (values (utf8->string encoded-string) (+ 2 encoded-string-length index)))

(define (decode-nullable-string encoded-val index)
  (define encoded-string-length (bytevector-s16-ref encoded-val index (endianness big)))

  (if (= -1 encoded-string-length)
      #f
      (decode-string encoded-val index)))

(define (decode-bytes encoded-val index)
  (define encoded-bytes-length (bytevector-s32-ref encoded-val index (endianness big)))
  (define encoded-bytes (make-bytevector encoded-bytes-length))

  (bytevector-copy! encoded-val (+ 4 index) encoded-bytes 0 encoded-bytes-length)

  (values encoded-bytes (+ 4 encoded-bytes-length)))

(define (decode-nullable-bytes encoded-val index)
  (define encoded-bytes-length (bytevector-s32-ref encoded-val index (endianness big)))

  (if (= -1 encoded-bytes-length)
      (values #f (+ 4 index))
      (decode-bytes encoded-val index)))

(define (decode-array schema encoded-val index)
  (define encoded-array-length (bytevector-s32-ref encoded-val index (endianness big)))
  (let array-loop ((index (+ 4 index))
                   (count encoded-array-length)
                   (array-values '()))
    (if (= 0 count)
        (values (cons (reverse array-values) '()) index)
        (match (car schema)
          ((_ ((_ . _) ...))
           (receive (decoded-value new-index)
               (decode-schema (cons (car schema) '()) encoded-val index)
             (array-loop new-index (- count 1) (cons decoded-value array-values))))
          (else
           (format #t "NOOO: ~a\n" schema)
           (receive (decoded-value new-index)
               (decode-type (car single) encoded-val index)
             (array-loop new-index (- count 1) (cons decoded-value array-values))))))))

(define (decode-type type encoded-val index)
  (match type
    ;;; TODO: add rest of types
    ('sint16 (decode-sint16 encoded-val index))
    ('sint32 (decode-sint32 encoded-val index))
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

(define (decode-request request-schema bv)
  (decode-schema request-schema bv 0))
