use leptos::prelude::*;

use crate::component::{
    BuildLogPanel, Callout, CheckPanel, MissingPipeline, PipelineCards, PipelineHeader, Shell,
    Sidebar, Sources, ViewToggle,
};
use crate::pipelines::{PipelineLocation, PipelineOverview};
use crate::route::AppRoute;

#[component]
pub fn PipelinesScreen(route: AppRoute, pipelines: Vec<PipelineLocation>) -> impl IntoView {
    view! {
        <Shell route=route.clone() title=route.section().label().to_owned()>
            <Sidebar route=route.clone() pipelines=pipelines.clone() />
            <main>
                <PipelineCards route=route pipelines=pipelines />
            </main>
        </Shell>
    }
}

#[component]
pub fn BuildLogScreen(
    route: AppRoute,
    pipelines: Vec<PipelineLocation>,
    pipeline: Option<PipelineOverview>,
) -> impl IntoView {
    let content = match pipeline {
        Some(pipeline) => view! {
            <PipelineHeader
                route=route.clone()
                directory=Some(pipeline.location().directory().to_owned())
            />
            <ViewToggle route=route.clone() />
            <Sources
                route=route.clone()
                pipeline=pipeline
                panel=|shown| view! { <BuildLogPanel shown=shown /> }.into_any()
            />
        }
        .into_any(),
        None => view! { <MissingPipeline route=route.clone() /> }.into_any(),
    };

    view! {
        <Shell route=route.clone() title=route.pipeline().unwrap_or_default().to_owned()>
            <Sidebar route=route pipelines=pipelines />
            <main>{content}</main>
        </Shell>
    }
}

#[component]
pub fn CheckScreen(
    route: AppRoute,
    pipelines: Vec<PipelineLocation>,
    pipeline: Option<PipelineOverview>,
) -> impl IntoView {
    let content = match pipeline {
        Some(pipeline) => view! {
            <PipelineHeader
                route=route.clone()
                directory=Some(pipeline.location().directory().to_owned())
            />
            <Sources
                route=route.clone()
                pipeline=pipeline
                panel=|shown| view! { <CheckPanel shown=shown /> }.into_any()
            />
        }
        .into_any(),
        None => view! { <MissingPipeline route=route.clone() /> }.into_any(),
    };

    view! {
        <Shell route=route.clone() title=route.pipeline().unwrap_or_default().to_owned()>
            <Sidebar route=route pipelines=pipelines />
            <main>{content}</main>
        </Shell>
    }
}

#[component]
pub fn UnreadScreen(route: AppRoute, cause: String) -> impl IntoView {
    view! {
        <Shell route=route title="Unread".to_owned()>
            <main>
                <Callout kind="error">{cause}</Callout>
            </main>
        </Shell>
    }
}
