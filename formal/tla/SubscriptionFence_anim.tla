-------------------------- MODULE SubscriptionFence_anim --------------------------
(***************************************************************************)
(* Spectacle animation view for SubscriptionFence.tla (issue #41 part B).   *)
(*                                                                         *)
(* Spectacle (will62794/spectacle) auto-loads an adjacent `<Spec>_anim.tla` *)
(* that EXTENDS the spec + the CommunityModules SVG module and defines an    *)
(* `AnimView` operator: an SVG element computed from the spec's state        *)
(* variables. Stepping the spec forward/backward in the Spectacle trace      *)
(* explorer re-renders AnimView, animating the protocol; any trace is        *)
(* shareable by URL. See spectacle/README.md for the exact load recipe and   *)
(* the curated crash-window / rotate-vs-coalesce walkthroughs.               *)
(*                                                                         *)
(* WHAT THIS DRAWS, per subscription s (a row):                             *)
(*   * a PHASE box  : idle (gray) / waking (amber) / live (green) -- the     *)
(*       arm_wake/claim/ack phase the Lua hash is in;                        *)
(*   * the FENCE    : the current (generation, wake_id) register, plus the   *)
(*       recorded holder and whether a lease is set;                         *)
(*   * a CRASH-WINDOW badge: W1/W2/W3/W4 when the state sits in one of the    *)
(*       four non-atomic recovery windows (WindowW1..W4 of the base spec);   *)
(*   * a ROTATE-vs-COALESCE badge: what the NEXT claim on s would do --       *)
(*       COALESCE (reuse the in-flight (gen,wake), amber) when phase=waking   *)
(*       with a wake set, else ROTATE (mint a fresh fence, blue);            *)
(*   * one TOKEN circle per worker w: GREEN = w currently holds an           *)
(*       ACK-ACCEPTABLE token (AckAcceptable(w,s)); RED-OUTLINE = w holds a   *)
(*       stale/fenced token; hollow = w holds no token. SingleHolder is      *)
(*       exactly "never two GREEN circles in one row" -- a violation would   *)
(*       be visually unmistakable.                                           *)
(***************************************************************************)
EXTENDS TLC, Naturals, Sequences, FiniteSets, SVG, SubscriptionFence

\* A deterministic ordering of the (symmetric) Subs / Workers sets so the
\* layout is stable frame to frame.
IsInj(f) == \A a, b \in DOMAIN f : f[a] = f[b] => a = b
SubOrder    == CHOOSE f \in [1..Cardinality(Subs)    -> Subs]    : IsInj(f)
WorkerOrder == CHOOSE f \in [1..Cardinality(Workers) -> Workers] : IsInj(f)
NSub == Cardinality(Subs)
NWk  == Cardinality(Workers)

\* --- palette ------------------------------------------------------------
PhaseColor(p) == IF p = "idle"   THEN "#cfd8dc"   \* blue-gray
                 ELSE IF p = "waking" THEN "#ffb74d"  \* amber
                 ELSE "#66bb6a"                       \* green (live)

\* Vertical position of subscription i's row.
RowY(i) == 30 + (i - 1) * 95

\* --- per-subscription row -----------------------------------------------
\* The phase box + the fence text.
PhaseBox(i) ==
    LET s == SubOrder[i] IN
    Group(
      << Rect(0, RowY(i), 150, 60,
            ("rx" :> "8" @@ "fill" :> PhaseColor(sub[s].phase)
                 @@ "stroke" :> "#37474f" @@ "stroke-width" :> "2")),
         Text(8, RowY(i) + 18, ToString(s) \o "  [" \o sub[s].phase \o "]",
            ("fill" :> "#102027" @@ "font-size" :> "13" @@ "font-weight" :> "bold")),
         Text(8, RowY(i) + 36,
            "gen=" \o ToString(sub[s].gen) \o "  wake=" \o ToString(sub[s].wake),
            ("fill" :> "#102027" @@ "font-size" :> "12")),
         Text(8, RowY(i) + 52,
            "holder=" \o ToString(sub[s].holder)
                \o (IF sub[s].lease_until # 0 THEN "  lease" ELSE ""),
            ("fill" :> "#102027" @@ "font-size" :> "11")) >>,
      <<>>)

\* The rotate-vs-coalesce badge: what the NEXT claim on s would decide.
RotCoalBadge(i) ==
    LET s     == SubOrder[i]
        coal  == ~ClaimRotatesFence(sub[s].phase, sub[s].wake)
        label == IF coal THEN "next claim: COALESCE" ELSE "next claim: ROTATE"
        col   == IF coal THEN "#ffb74d" ELSE "#42a5f5" IN
    Group(
      << Rect(160, RowY(i) + 4, 150, 20,
            ("rx" :> "4" @@ "fill" :> col @@ "stroke" :> "#263238")),
         Text(166, RowY(i) + 18, label,
            ("fill" :> "#0d1b2a" @@ "font-size" :> "11" @@ "font-weight" :> "bold")) >>,
      <<>>)

\* The crash-window badge (W1..W4) when s sits in a non-atomic recovery window.
WindowOf(s) ==
    IF /\ sub[s].dispatch = "pullwake" /\ sub[s].phase = "waking"
       /\ sub[s].wake_sent = 0 /\ pending[s] = "emit"          THEN "W1 arm-before-emit"
    ELSE IF pending[s] = "stamp"                                THEN "W2 commit-then-stamp"
    ELSE IF /\ sub[s].dispatch = "pullwake" /\ sub[s].phase = "waking"
            /\ sub[s].wake_sent = 1 /\ sub[s].lease_until = 0   THEN "W3 post-emit T4"
    ELSE IF /\ sub[s].phase = "live" /\ sub[s].lease_until # 0
            /\ (\A w \in Workers : ~AckAcceptable(w, s))
            /\ leaseMem[s]                                      THEN "W4 claim-before-ack"
    ELSE ""

WindowBadge(i) ==
    LET s == SubOrder[i] win == WindowOf(SubOrder[i]) IN
    IF win = "" THEN Group(<<>>, <<>>)
    ELSE Group(
      << Rect(160, RowY(i) + 30, 150, 22,
            ("rx" :> "4" @@ "fill" :> "#ef5350" @@ "stroke" :> "#b71c1c")),
         Text(166, RowY(i) + 45, win,
            ("fill" :> "#ffffff" @@ "font-size" :> "11" @@ "font-weight" :> "bold")) >>,
      <<>>)

\* One token circle per worker, GREEN iff that worker is ack-acceptable for s.
TokenCircle(i, j) ==
    LET s == SubOrder[i] w == WorkerOrder[j]
        cx == 340 + (j - 1) * 70
        cy == RowY(i) + 30
        ackok == AckAcceptable(w, s)
        held  == token[w][s].held
        fill  == IF ackok THEN "#43a047"            \* green: ack-acceptable
                 ELSE IF held THEN "#ffffff"         \* hollow w/ red rim: stale token
                 ELSE "#eceff1"                      \* light: no token
        rim   == IF ackok THEN "#1b5e20"
                 ELSE IF held THEN "#c62828" ELSE "#b0bec5" IN
    Group(
      << Circle(cx, cy, 18,
            ("fill" :> fill @@ "stroke" :> rim @@ "stroke-width" :> "3")),
         Text(cx - 9, cy - 22, ToString(w),
            ("fill" :> "#263238" @@ "font-size" :> "11")),
         Text(cx - 16, cy + 4,
            IF held THEN "g" \o ToString(token[w][s].gen) \o "/w" \o ToString(token[w][s].wake)
                    ELSE "--",
            ("fill" :> (IF ackok THEN "#ffffff" ELSE "#37474f") @@ "font-size" :> "10")) >>,
      <<>>)

WorkerTokens(i) ==
    Group([ j \in 1..NWk |-> TokenCircle(i, j) ], <<>>)

SubRow(i) ==
    Group(<< PhaseBox(i), RotCoalBadge(i), WindowBadge(i), WorkerTokens(i) >>, <<>>)

\* --- legend -------------------------------------------------------------
Legend ==
    Group(
      << Circle(12, 12, 9, ("fill" :> "#43a047" @@ "stroke" :> "#1b5e20" @@ "stroke-width" :> "3")),
         Text(28, 16, "ack-acceptable token (a holder)", ("fill" :> "#263238" @@ "font-size" :> "11")),
         Circle(12, 36, 9, ("fill" :> "#ffffff" @@ "stroke" :> "#c62828" @@ "stroke-width" :> "3")),
         Text(28, 40, "stale/fenced token (inert)", ("fill" :> "#263238" @@ "font-size" :> "11")),
         Text(300, 16, "SingleHolder = never two green circles in a row",
            ("fill" :> "#b71c1c" @@ "font-size" :> "12" @@ "font-weight" :> "bold")) >>,
      ("transform" :> "translate(20, 8)"))

AnimView ==
    Group(
      << Legend, Group([ i \in 1..NSub |-> SubRow(i) ], ("transform" :> "translate(20, 50)")) >>,
      ("transform" :> "translate(10, 10)"))

\* --- TLC animation alias: emit one SVG frame per state so a curated trace   *)
\* can be rendered headless (no browser) for a shareable filmstrip via        *)
\* `make spectacle-frames`. (Spectacle itself renders AnimView live.)         *)
AnimAlias ==
    [ phase    |-> [ s \in Subs |-> sub[s].phase ],
      gen      |-> [ s \in Subs |-> sub[s].gen ],
      wake     |-> [ s \in Subs |-> sub[s].wake ],
      holder   |-> [ s \in Subs |-> sub[s].holder ],
      window   |-> [ s \in Subs |-> WindowOf(s) ] ]
    @@ [ _anim |-> SVGSerialize(
            SVGDoc(AnimView, 0, 0, 560, 50 + NSub * 95, <<>>),
            "SubscriptionFence_anim_", TLCGet("level")) ]

=============================================================================
