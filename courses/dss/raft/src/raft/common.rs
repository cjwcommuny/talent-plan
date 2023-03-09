use std::ops::Deref;
use std::panic;
use tracing::error;

pub fn last_index_and_element<T>(slice: &[T]) -> Option<(usize, &T)> {
    slice.iter().enumerate().rev().next()
}

/// When panic occurs, log it
pub fn set_panic_with_log() {
    panic::set_hook(Box::new(|panic_info| {
        let (filename, line) = panic_info
            .location()
            .map(|loc| (loc.file(), loc.line()))
            .unwrap_or(("<unknown>", 0));

        let cause = panic_info
            .payload()
            .downcast_ref::<String>()
            .map(String::deref);

        let cause = cause.unwrap_or_else(|| {
            panic_info
                .payload()
                .downcast_ref::<&str>()
                .map(|s| *s)
                .unwrap_or("<cause unknown>")
        });

        error!("A panic occurred at {}:{}: {}", filename, line, cause);
    }));
}

/// Just for warning
pub fn side_effect<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    f()
}
