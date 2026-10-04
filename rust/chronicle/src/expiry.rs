//! Immutable expiry policy; clock samples are inputs to replicated commands.
use serde::{Deserialize, Deserializer, Serialize};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Expiry {
    Ttl(u64),
    At { seconds: i64, nanos: u32 },
}

impl Expiry {
    pub fn parse(ttl: Option<&str>, absolute: Option<&str>) -> Result<Option<Self>, &'static str> {
        match (ttl, absolute) {
            (Some(_), Some(_)) => Err("TTL and absolute expiry are mutually exclusive"),
            (Some(value), None) => {
                if value.is_empty()
                    || !value.bytes().all(|b| b.is_ascii_digit())
                    || (value.len() > 1 && value.starts_with('0'))
                {
                    return Err("TTL must be a canonical nonnegative decimal integer");
                }
                let seconds = value
                    .parse::<i64>()
                    .map_err(|_| "TTL exceeds signed 64-bit range")?;
                Ok(Some(Self::Ttl(seconds as u64)))
            }
            (None, Some(value)) => {
                // time's parser accepts any separator; the wire grammar does not.
                if !matches!(value.as_bytes().get(10), Some(b'T' | b't')) {
                    return Err("invalid RFC3339 expiry separator");
                }
                let time =
                    OffsetDateTime::parse(value, &Rfc3339).map_err(|_| "invalid RFC3339 expiry")?;
                Ok(Some(Self::At {
                    seconds: time.unix_timestamp(),
                    nanos: time.nanosecond(),
                }))
            }
            (None, None) => Ok(None),
        }
    }

    pub fn expired(self, access_ms: u64, now_ms: u64) -> bool {
        match self {
            Self::Ttl(seconds) => {
                u128::from(now_ms) > u128::from(access_ms) + u128::from(seconds) * 1000
            }
            Self::At { seconds, nanos } => {
                i128::from(now_ms) * 1_000_000
                    > i128::from(seconds) * 1_000_000_000 + i128::from(nanos)
            }
        }
    }

    pub fn absolute_header(self) -> Option<String> {
        let Self::At { seconds, nanos } = self else {
            return None;
        };
        OffsetDateTime::from_unix_timestamp(seconds)
            .ok()?
            .replace_nanosecond(nanos)
            .ok()?
            .format(&Rfc3339)
            .ok()
    }

    /// Preserve the guard on legacy committed expiry-delete commands.
    pub fn fixed_millis(self) -> Option<u64> {
        let Self::At { seconds, nanos } = self else {
            return None;
        };
        if nanos % 1_000_000 != 0 {
            return None;
        }
        u64::try_from(seconds)
            .ok()?
            .checked_mul(1000)?
            .checked_add(u64::from(nanos / 1_000_000))
    }
}

/// Old snapshots/logs retain their fixed deadline; their original TTL was not stored.
pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
    decoder: D,
) -> Result<Option<Expiry>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Saved {
        Current(Expiry),
        Millis(u64),
    }
    Ok(
        Option::<Saved>::deserialize(decoder)?.map(|saved| match saved {
            Saved::Current(policy) => policy,
            Saved::Millis(ms) => Expiry::At {
                // Even u64::MAX milliseconds fits signed 64-bit seconds.
                seconds: (ms / 1000) as i64,
                nanos: ((ms % 1000) * 1_000_000) as u32,
            },
        }),
    )
}
