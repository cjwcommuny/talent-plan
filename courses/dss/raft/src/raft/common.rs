pub fn last_index_and_element<T>(slice: &[T]) -> Option<(usize, &T)> {
    slice.iter().enumerate().rev().next()
}
