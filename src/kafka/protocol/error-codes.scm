(define-module (kafka protocol error-codes)
  #:export (error-codes))

(define error-codes
  '((-1 . (unknown-server-error . #f))
    (0  . (none . #f))
    (1  . (offset-out-of-range . #f))
    (2  . (corrupt-message . #t))
    (3  . (unknown-topic-or-partition . #t))
    (4  . (invalid-fetch-size . #f))
    (5  . (leader-not-available . #t))
    (6  . (not-leader-for-partition . #t))
    (7  . (request-timed-out . #t))
    (8  . (broker-not-available . #f))
    (9  . (replica-not-available . #f))
    (10 . (message-too-large . #f))
    (11 . (stale-controller-epock . #f))
    (12 . (offset-metadata-too-large . #f))
    (13 . (network-exception . #t))
    (14 . (coordinator-load-in-progress . #t))
    (15 . (coordinator-not-available . #t))
    (16 . (not-coordinator . #t))
    (17 . (invalid-topic-exception . #f))
    (18 . (record-list-too-large . #f))
    (19 . (not-enough-replicas . #t))
    (20 . (not-enough-replicas-after-append . #t))
    (21 . (invalid-required-acks . #f))
    (22 . (illegal-generation . #f))
    (23 . (inconsistent-group-protocol . #f))
    (24 . (invalid-group-id . #f))
    (25 . (unknown-member-id . #f))
    (26 . (invalid-session-timeout . #f))
    (27 . (rebalance-in-progress . #f))
    (28 . (invalid-commit-offset-size . #f))
    (29 . (topic-authorization-failed . #f))
    (30 . (group-authorization-failed . #f))
    (31 . (cluster-authorization-failed . #f))
    (32 . (invalid-timestamp . #f))
    (33 . (unsupported-sasl-mechanism . #f))
    (34 . (illegal-sasl-state . #f))
    (35 . (unsupported-version . #f))
    (36 . (topic-already-exists . #f))
    (37 . (invalid-partitions . #f))
    (38 . (invalid-replication-factor . #f))
    (39 . (invalid-replica-assignment . #f))
    (40 . (invalid-config . #f))
    (41 . (not-controller . #t))
    (42 . (invalid-request . #f))
    (43 . (unsupported-for-message-format . #f))
    (44 . (policy-violation . #f))
    (45 . (out-of-order-sequence . #f))
    (46 . (duplicate-sequence-number . #f))
    (47 . (invalid-producer-epoch . #f))
    (48 . (invalid-txn-state . #f))
    (49 . (invalid-producer-id-map . #f))
    (50 . (invalid-transation-timeout . #f))
    (51 . (concurrent-transactions . #f))
    (52 . (transaction-coordinator-fenced . #f))
    (53 . (transactional-id-authorization-failed . #f))
    (54 . (security-disabled . #f))
    (55 . (operation-not-attempted . #f))
    (56 . (kafka-storage-error . #t))
    (57 . (log-dir-not-found . #f))
    (58 . (sasl-authentication-failed . #f))
    (59 . (unknown-producer-id . #f))
    (60 . (reassignment-in-progress . #f))
    (61 . (delegation-token-auth-disabled . #f))
    (62 . (delegation-token-not-found . #f))
    (63 . (delegation-token-owner-mismatch . #f))
    (64 . (delegation-token-request-not-allowed . #f))
    (65 . (delegation-token-authorization-failed . #f))
    (66 . (delegation-token-expired . #f))
    (67 . (invalid-principal-type . #f))
    (68 . (non-empty-group . #f))
    (69 . (group-id-not-found . #f))
    (70 . (fetch-session-id-not-found . #t))
    (71 . (invalid-fetch-session-epoch . #t))
    (72 . (listener-not-found . #t))
    (73 . (topic-deletion-disabled . #f))
    (74 . (fenced-leader-epoch . #t))
    (75 . (unknown-leader-epoch . #t))
    (76 . (unsupported-compression-type . #f))
    (77 . (stale-broker-epoch . #f))
    (78 . (offset-not-available . #t))
    (79 . (member-id-required . #f))
    (80 . (preferred-leader-not-available . #t))
    (81 . (group-max-size-reached . #f))
    (82 . (fenced-instance-id . #f))))
