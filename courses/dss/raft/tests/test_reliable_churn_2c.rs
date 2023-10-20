use crate::common::{init_logger, internal_churn};
use function_name::named;

mod common;

#[test]
#[named]
fn test_reliable_churn_2c() {
    init_logger(function_name!());
    internal_churn(false);
}
