-- Typed transcriptions (P0.5 skeleton, issue #29)
import Chronicle.Producer
import Chronicle.Offset
import Chronicle.OffsetGreater
import Chronicle.Cursor
import Chronicle.Webhook

-- Pure-core proofs (P1.1, issue #30)
import Chronicle.Producer.Proofs
import Chronicle.Offset.Proofs
import Chronicle.Cursor.Proofs
import Chronicle.Webhook.Proofs
import Chronicle.Fence.SingleHolder

-- Axiom audit (the anti-`sorry` / TCB gate, issue #30 acceptance criteria)
import Chronicle.Axioms

/-!
# Chronicle pure-core transcriptions + proofs

Umbrella import for the typed transcription of Chronicle's pure cores (issue #29)
and their function-level correctness proofs (issue #30 / P1.1). Importing this
module compiles every transcription and every proof, so `lake build` is the CI
oracle. The `#print axioms` gate in `Chronicle/Axioms.lean` confirms no theorem
depends on `sorry`.
-/
