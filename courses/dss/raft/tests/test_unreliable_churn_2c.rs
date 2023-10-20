use crate::common::{init_logger, internal_churn};
use function_name::named;

pub mod common;

#[test]
#[named]
fn test_unreliable_churn_2c() {
    init_logger(function_name!());
    internal_churn(true);
}
