#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_fn_trait_return)]
#![feature(let_chains)]
#![feature(result_option_inspect)]
#[allow(unused_imports)]
#[macro_use]
extern crate log;
#[allow(unused_imports)]
#[macro_use]
extern crate prost_derive;

pub mod kvraft;
mod proto;
pub mod raft;

/// A place holder for suppressing `unused_variables` warning.
fn your_code_here<T>(_: T) -> ! {
    unimplemented!()
}
