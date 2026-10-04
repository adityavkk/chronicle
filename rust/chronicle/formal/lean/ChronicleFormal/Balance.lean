import Std

namespace ChronicleFormal.Balance

-- For fixed measurements, the admission hysteresis strictly lowers the
-- nonnegative integer potential. Changing measurements need not converge.
theorem hysteresis_descends (before after : Nat) (positive : 0 < before)
    (improvement : after * 10 ≤ before * 9) : after < before := by
  omega

-- Replicated cooldown holds even when two controllers observed the old state:
-- once the first proposal applies, the second cannot reuse its timestamp.
theorem cooldown_rejects_same_time (time delay : Nat) (positive : 0 < delay) :
    ¬ (time + delay ≤ time) := by
  omega

end ChronicleFormal.Balance
