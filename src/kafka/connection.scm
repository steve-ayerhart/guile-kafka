(define-module (kafka connection)
  #:use-module (kafka protocol messages)

  #:use-module (srfi srfi-1)

  #:use-module (rnrs bytevectors)
  #:use-module (ice-9 binary-ports)
  #:use-module (ice-9 match))

(define (broker-connect host port)
  (define addresses
    (delete-duplicates
     (getaddrinfo host (if (number? port) (number->string port) port) AI_NUMERICSERV)
     (λ (a1 a2)
       (equal? (addrinfo:addr a1) (addrinfo:addr a2)))))

  (define (open-socket)
    (let address-loop ((addresses addresses))
      (let* ((ai (car addresses))
             (sock (with-fluids ((%default-port-encoding #f))
                     (socket PF_INET SOCK_STREAM 0))))

        (setsockopt sock IPPROTO_TCP TCP_NODELAY 1)
        (fcntl sock F_SETFL (logior O_NONBLOCK (fcntl sock F_GETFL)))
        (setvbuf sock 'block 1024)

        (catch 'system-error
          (λ ()
            (connect sock (addrinfo:addr ai))

            sock)

          (λ args
            (close sock)
            (if (null? (cdr addresses))
                (apply throw args)
                (address-loop (cdr addresses))))))))

  (open-socket))
