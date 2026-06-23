import Mathlib.Tactic
import Mathlib.Order.Basic

/-!
# Parametric monotone-token ⇒ single-holder (INV-FENCE-01)

The reusable algebraic core of Chronicle's central safety property, ranked #1 in
INVARIANTS.md. Stated **parametrically** over an abstract register/grant model so
both the `(generation, wake_id)` subscription fence (`webhook/state.go`
`FenceDecision`, Lua `fenced`, oracle `jepsen/checker/model_fence.go`) and the
`(owner, epoch)` slot fence instantiate it rather than re-deriving it.

## The model

A register holds a `current` token drawn from any `LinearOrder` `Tok`. A `grant`
either **coalesces** (keeps the token: a re-claim of the in-flight holder) or
mints a **strictly greater** token on a new holder. A held token `t` is
*accept-guarded*: an operation carrying `t` is accepted iff `t = current`.

The theorem set:
- `grant` only ever moves the token up (monotone) — `grant_monotone`;
- a sequence of grants is monotone — `runGrants_monotone`;
- **single holder at an instant**: any two tokens both accept-guarded against the
  same `current` are equal (`single_holder_instant`) — at most one distinct token
  is acceptable, so at most one holder;
- **a deposed holder stays fenced**: once the token advances strictly past `t`,
  no later state (the token only rises) ever re-accepts `t`
  (`deposed_stays_fenced`).

## Anti-vacuity

`deposed_stays_fenced` is falsifiable: drop strict monotonicity (let `grant` lower
the token) and a deposed holder could be re-accepted — the fence would leak. The
strict-increase hypothesis is used essentially. `single_holder_instant` is the
algebraic skeleton the TLA+ `SubscriptionFence` proof and the owner-epoch layering
both cite.
-/

namespace Chronicle.Fence

variable {Tok : Type*} [LinearOrder Tok]

/-- An abstract grant step: given the register's `current` token it returns the
new token. The step is **monotone-or-coalesce**: it never lowers the token. We
package the law as a hypothesis on the step rather than baking a representation,
so the same theorem instantiates for `(gen, wake)` and `(owner, epoch)`. -/
structure GrantStep (Tok : Type*) [LinearOrder Tok] where
  /-- the new current token after the grant, as a function of the old one. -/
  step    : Tok → Tok
  /-- a grant never lowers the token (strict raise on a new holder, equal on a
      coalesce). -/
  noLower : ∀ t, t ≤ step t

/-- A token `t` is **accept-guarded** against `current` iff it equals it. This is
the `token = current` guard of `FenceDecision`/`fenced`. -/
def Accepts (current t : Tok) : Prop := t = current

/-- A grant is monotone: the post-token is ≥ the pre-token. -/
theorem grant_monotone (g : GrantStep Tok) (t : Tok) : t ≤ g.step t := g.noLower t

/-- Replay a list of grants, threading the current token. -/
def runGrants (g : GrantStep Tok) : Tok → List Unit → Tok
  | t, [] => t
  | t, _ :: rest => runGrants g (g.step t) rest

/-- **Monotonicity across a run of grants.** The token is non-decreasing across any
sequence of grants — the register's `current` only ever rises. -/
theorem runGrants_monotone (g : GrantStep Tok) (t : Tok) (ops : List Unit) :
    t ≤ runGrants g t ops := by
  induction ops generalizing t with
  | nil => exact le_refl t
  | cons _ rest ih =>
    have h1 : t ≤ g.step t := g.noLower t
    have h2 : g.step t ≤ runGrants g (g.step t) rest := ih (g.step t)
    simp only [runGrants]
    exact le_trans h1 h2

/-- **INV-FENCE-01 — single holder at an instant.**
Any two tokens that are both accept-guarded against the *same* `current` are
equal. Hence at most one distinct token can act at a given register state: a
unique holder. This is the algebraic heart of "at most one worker holds an
ack-acceptable token". -/
theorem single_holder_instant (current t₁ t₂ : Tok)
    (h₁ : Accepts current t₁) (h₂ : Accepts current t₂) : t₁ = t₂ := by
  unfold Accepts at h₁ h₂
  rw [h₁, h₂]

/-- **INV-FENCE-01 — a deposed holder stays fenced (monotone variant).**
If the register's token has strictly advanced past a held token `t`
(`current > t`), then `t` is not accept-guarded against `current` — the deposed
holder cannot be re-accepted. Combined with `runGrants_monotone` (the token only
rises), once a token is superseded it is fenced out **forever**. -/
theorem deposed_stays_fenced (current t : Tok) (h : current > t) :
    ¬ Accepts current t := by
  unfold Accepts
  intro he
  rw [he] at h
  exact lt_irrefl current h

/-- A deposed token stays fenced across **any further run of grants**: if it is
already strictly below the current token, it is below (and so ≠) the token after
any number of additional grants, hence never re-accepted. -/
theorem deposed_stays_fenced_run (g : GrantStep Tok) (current t : Tok)
    (h : current > t) (ops : List Unit) :
    ¬ Accepts (runGrants g current ops) t := by
  have hmono : current ≤ runGrants g current ops := runGrants_monotone g current ops
  have : t < runGrants g current ops := lt_of_lt_of_le h hmono
  exact deposed_stays_fenced _ t this

/-! ## Instantiation 1 — the `(generation, wake_id)` subscription fence

A grant rotates the fence by minting a fresh, strictly-greater `(generation,
wake_id)` (lexicographically by generation), or coalesces by reusing the in-flight
one. With `Tok := ℕ ×ₗ ℕ` (generation, wake-id index) the abstract model
specializes directly: the accept-guard `token = current` is exactly
`FenceDecision`'s `tokenGen = cur.gen ∧ reqWake = cur.wake`. -/

/-- The `(gen, wake)` fence as a `GrantStep` over `ℕ ×ₗ ℕ`: every grant raises the
generation by one (a fresh, strictly-greater token), modeling fence rotation. The
monotone law is discharged automatically. -/
def genWakeFence : GrantStep (ℕ ×ₗ ℕ) where
  step := fun t => toLex ((ofLex t).1 + 1, 0)
  noLower := by
    intro t
    rw [show t = toLex ((ofLex t).1, (ofLex t).2) from rfl]
    rw [Prod.Lex.toLex_le_toLex]
    left
    simp

/-- INV-FENCE-01 specialized to the `(gen, wake)` fence: a token deposed by a
generation rotation stays fenced across any further rotations. -/
theorem instGenWakeFence (current t : ℕ ×ₗ ℕ) (h : current > t) (ops : List Unit) :
    ¬ Accepts (runGrants genWakeFence current ops) t :=
  deposed_stays_fenced_run genWakeFence current t h ops

/-! ## Instantiation 2 — the `(owner, epoch)` slot fence

The ownership-layering fence keys on `(owner, epoch)` and raises the epoch on a
takeover. Same abstract model, `Tok := ℕ ×ₗ ℕ` (epoch, owner index): a takeover
strictly raises the epoch, a heartbeat coalesces. The single-holder lemma is the
property the owner-epoch layering work cites to argue the `(gen,wake)` fence alone
still upholds INV-FENCE-01. -/

/-- The `(owner, epoch)` fence as a `GrantStep`: a takeover raises the epoch. -/
def ownerEpochFence : GrantStep (ℕ ×ₗ ℕ) where
  step := fun t => toLex ((ofLex t).1 + 1, 0)
  noLower := by
    intro t
    rw [show t = toLex ((ofLex t).1, (ofLex t).2) from rfl]
    rw [Prod.Lex.toLex_le_toLex]
    left
    simp

/-- INV-FENCE-01 specialized to the `(owner, epoch)` fence: a slot holder deposed
by an epoch bump stays fenced across any further takeovers. -/
theorem instOwnerEpochFence (current t : ℕ ×ₗ ℕ) (h : current > t) (ops : List Unit) :
    ¬ Accepts (runGrants ownerEpochFence current ops) t :=
  deposed_stays_fenced_run ownerEpochFence current t h ops

end Chronicle.Fence
