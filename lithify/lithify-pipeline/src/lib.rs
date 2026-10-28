// No future here is asked to be Send: a run steps one sink at a time on one thread, and a
// DuckDB connection cannot be shared across threads.
#![allow(async_fn_in_trait)]

pub mod artifact;
pub mod build_log;
pub mod check;
pub mod infra;
pub mod pipeline;
pub mod place;
pub mod progress;
pub mod runner;
pub mod schedule;
pub mod stepping;
pub mod table;
pub mod unreadable;
pub mod warehouse;

pub use artifact::{Declaration, Declarations, Misdeclaring};
pub use pipeline::Pipeline;
pub use place::{Placed, Places, Placing};
pub use unreadable::{Cause, Unreadable};
