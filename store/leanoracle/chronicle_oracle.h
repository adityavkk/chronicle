/* chronicle_oracle.h — C ABI for the proven Lean producer/offset oracle.
 *
 * Third differential oracle (P1.2, issue #31). These entry points are emitted by
 * Lean's C backend from `Chronicle/Extern.lean` over the proven, byte-identical
 * copies of `Chronicle/Producer.lean` and `Chronicle/Offset.lean`. The static
 * archive `libchronicle_oracle.a` next to this header is the VENDORED compiled C;
 * routine Go CI links it with NO Lean toolchain present. Regenerate both with
 * `store/leanoracle/scripts/build-lean-oracle.sh`; the recorded toolchain and
 * source commit are in `store/leanoracle/PROVENANCE.txt`.
 *
 * Runtime init contract: before calling any entry point below, call exactly once
 *   lean_initialize_runtime_module();
 *   initialize_chronicleoracle_Chronicle_Extern(1);  // returns a lean IO result
 *   lean_io_mark_end_initialization();
 * The package-name prefix `chronicleoracle_` on the module initializer comes from
 * the lake project name in `store/leanoracle/lean/lakefile.toml`.
 *
 * ABI: scalar-only. Inputs/outputs are fixed-width C integers; no `lean_object`
 * crosses the boundary, so there is no ref-counting or heap traffic on a call.
 */

#ifndef CHRONICLE_ORACLE_H
#define CHRONICLE_ORACLE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Lean runtime bring-up (defined in libleanrt.a). */
void lean_initialize_runtime_module(void);
void lean_io_mark_end_initialization(void);

/* Module initializer (emitted by the Lean C backend). Returns a Lean IO result
 * object; the caller must check it is not an error and then drop its ref. */
void *initialize_chronicleoracle_Chronicle_Extern(uint8_t builtin);

/* lean_offset_compare: Offset.compare lexicographic order on (readSeq, byteOffset).
 * Returns -1 / 0 / 1 in an int8_t (Lean `Int8`; widen as SIGNED in the caller). */
int8_t lean_offset_compare(uint64_t a_read_seq, uint64_t a_byte_offset,
                           uint64_t b_read_seq, uint64_t b_byte_offset);

/* ValidateProducer reply tuple, one accessor per field over the flattened
 * request. `state_present`: 0 = first contact (Go state == nil), 1 = existing
 * state (st_epoch / st_last_seq then read). epoch / seq / now are the request
 * triple and injected clock. All take the same six inputs.
 *
 *   result: 0 none, 1 accepted, 2 duplicate (store.ProducerResult*)
 *   error:  0 nil, 1 seqGap, 2 staleEpoch, 3 invalidEpochSeq
 *           (nil / ErrProducerSeqGap / ErrStaleEpoch / ErrInvalidEpochSeq)
 *   persist: 1 iff the Go core returns a non-nil *ProducerState, else 0
 *   new_epoch / new_last_seq: the persisted state fields when persist == 1
 */
uint8_t lean_validate_producer_result(uint8_t state_present, int64_t st_epoch,
                                      int64_t st_last_seq, int64_t epoch,
                                      int64_t seq, int64_t now);
uint8_t lean_validate_producer_error(uint8_t state_present, int64_t st_epoch,
                                     int64_t st_last_seq, int64_t epoch,
                                     int64_t seq, int64_t now);
int64_t lean_validate_producer_current_epoch(uint8_t state_present, int64_t st_epoch,
                                             int64_t st_last_seq, int64_t epoch,
                                             int64_t seq, int64_t now);
int64_t lean_validate_producer_expected_seq(uint8_t state_present, int64_t st_epoch,
                                            int64_t st_last_seq, int64_t epoch,
                                            int64_t seq, int64_t now);
int64_t lean_validate_producer_received_seq(uint8_t state_present, int64_t st_epoch,
                                            int64_t st_last_seq, int64_t epoch,
                                            int64_t seq, int64_t now);
int64_t lean_validate_producer_last_seq(uint8_t state_present, int64_t st_epoch,
                                        int64_t st_last_seq, int64_t epoch,
                                        int64_t seq, int64_t now);
uint8_t lean_validate_producer_persist(uint8_t state_present, int64_t st_epoch,
                                       int64_t st_last_seq, int64_t epoch,
                                       int64_t seq, int64_t now);
int64_t lean_validate_producer_new_epoch(uint8_t state_present, int64_t st_epoch,
                                         int64_t st_last_seq, int64_t epoch,
                                         int64_t seq, int64_t now);
int64_t lean_validate_producer_new_last_seq(uint8_t state_present, int64_t st_epoch,
                                            int64_t st_last_seq, int64_t epoch,
                                            int64_t seq, int64_t now);

#ifdef __cplusplus
}
#endif

#endif /* CHRONICLE_ORACLE_H */
