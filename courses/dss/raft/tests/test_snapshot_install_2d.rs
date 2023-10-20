use crate::common::{init_logger, snap_common};
use function_name::named;

pub mod common;

#[test]
#[named]
fn test_snapshot_install_2d() {
    init_logger(function_name!());
    snap_common(
        "Test (2D): install snapshots (disconnect)",
        true,
        true,
        false,
    );
}
