// No future here is asked to be Send: a run steps one sink at a time on one thread, and a
// DuckDB connection cannot be shared across threads.
#![allow(async_fn_in_trait)]

pub mod artifact;
pub mod build_log;
pub mod check;
pub mod era;
pub mod event;
pub mod listener;
pub mod model;
pub mod position;
pub mod progress;
pub mod runner;
pub mod schedule;
pub mod sink;
pub mod source;
pub mod sql;
pub mod stepping;
pub mod table_listeners;
pub mod ts;
pub mod window;
