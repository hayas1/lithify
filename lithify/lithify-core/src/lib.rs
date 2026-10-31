// No future here is asked to be Send: a run steps one sink at a time on one thread, and a
// DuckDB connection cannot be shared across threads.
#![allow(async_fn_in_trait)]

pub mod artifact;
pub mod build;
pub mod era;
pub mod event;
pub mod listener;
pub mod model;
pub mod position;
pub mod runner;
pub mod sink;
pub mod source;
pub mod sql;
pub mod table_listeners;
pub mod ts;
pub mod window;
