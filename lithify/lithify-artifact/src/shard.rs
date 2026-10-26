use chrono::{DateTime, Utc};
use chrono_tz::Tz;

use lithify_core::model::TableId;
use lithify_core::ts::{Event, Ts};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shard {
    format: String,
    zone: Tz,
}

impl Shard {
    pub fn new(format: &str, zone: Tz) -> Self {
        Self {
            format: format.to_owned(),
            zone,
        }
    }

    pub fn name(&self, table: &TableId, stamped: Ts<Event>) -> String {
        let stamped = DateTime::<Utc>::from(stamped).with_timezone(&self.zone);

        format!("{}_{}", table.table(), stamped.format(&self.format))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(rfc3339: &str) -> Ts<Event> {
        Ts::from(rfc3339.parse::<DateTime<Utc>>().unwrap())
    }

    #[test]
    fn a_shard_is_named_after_its_table_and_the_instant_it_is_stamped_with() {
        assert_eq!(
            Shard::new("%Y%m%d", Tz::UTC)
                .name(&TableId::new("shop", "people"), at("2026-09-21T00:00:00Z")),
            "people_20260921"
        );
    }

    #[test]
    fn a_shard_reads_the_instant_it_is_stamped_with_in_its_time_zone() {
        assert_eq!(
            Shard::new("%Y%m%d_%H%M%S", chrono_tz::Asia::Tokyo)
                .name(&TableId::new("shop", "people"), at("2026-09-21T00:00:00Z")),
            "people_20260921_090000"
        );
    }
}
