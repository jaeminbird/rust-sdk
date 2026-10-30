#![cfg(all(
    feature = "client",
    feature = "transport-streamable-http-client",
    feature = "transport-streamable-http-client-reqwest",
    not(feature = "local")
))]

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::{Body, Bytes},
    http::{HeaderMap, Response, StatusCode},
    routing::post,
};
use rmcp::{
    ServiceExt,
    model::CallToolRequestParams,
    transport::{
        StreamableHttpClientTransport,
        streamable_http_client::{StreamableHttpClient, StreamableHttpError},
    },
};

const SESSION_ID: &str = "old-session";

/// A server that hands out a session, then "restarts" mid-call: the tool call's
/// SSE stream carries a resumable event id and closes before the response, and
/// every GET for the old session answers 404, as a fresh process would.
async fn spawn_restarting_server(get_count: Arc<AtomicUsize>) -> String {
    let router = Router::new().route(
        "/mcp",
        post(|body: Bytes| async move {
            let message: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let id = message.get("id").cloned();
            match (message["method"].as_str(), id) {
                (Some("initialize"), Some(id)) => {
                    let version = message["params"]["protocolVersion"].clone();
                    let body = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {
                            "protocolVersion": version,
                            "capabilities": { "tools": {} },
                            "serverInfo": { "name": "restarting", "version": "1" }
                        }
                    });
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "application/json")
                        .header("mcp-session-id", SESSION_ID)
                        .body(Body::from(body.to_string()))
                        .unwrap()
                }
                (_, None) => Response::builder()
                    .status(StatusCode::ACCEPTED)
                    .body(Body::empty())
                    .unwrap(),
                (_, Some(_)) => Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/event-stream")
                    .body(Body::from("id: e1\nretry: 10\ndata:\n\n"))
                    .unwrap(),
            }
        })
        .get(move |headers: HeaderMap| {
            let get_count = get_count.clone();
            async move {
                get_count.fetch_add(1, Ordering::SeqCst);
                let status = if headers.contains_key("mcp-session-id") {
                    StatusCode::NOT_FOUND
                } else {
                    StatusCode::METHOD_NOT_ALLOWED
                };
                Response::builder()
                    .status(status)
                    .body(Body::empty())
                    .unwrap()
            }
        }),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://{addr}/mcp")
}

/// After a server restart, resuming a request's SSE stream gets 404 for the old
/// session. That is final (the spec says the client must start a new session),
/// so the pending call must fail instead of retrying the GET forever.
#[tokio::test]
async fn call_fails_when_resume_gets_404_for_session() {
    let get_count = Arc::new(AtomicUsize::new(0));
    let url = spawn_restarting_server(get_count.clone()).await;
    let client = ().serve(StreamableHttpClientTransport::from_uri(url)).await.expect("initialize");

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        client.call_tool(CallToolRequestParams::new("slow")),
    )
    .await;

    let result = result.expect("call must not hang when the session is gone");
    assert!(result.is_err(), "expected an error, got {result:?}");
    let gets = get_count.load(Ordering::SeqCst);
    assert!(
        gets <= 3,
        "a 404 for the session is final and must not be retried ({gets} GETs)"
    );
}

async fn spawn_404_server() -> String {
    let router = Router::new().route(
        "/mcp",
        axum::routing::get(|| async {
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
                .unwrap()
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://{addr}/mcp")
}

#[tokio::test]
async fn get_stream_maps_404_with_session_to_session_expired() {
    let url = spawn_404_server().await;
    let result = reqwest::Client::new()
        .get_stream(
            Arc::from(url.as_str()),
            Some(Arc::from(SESSION_ID)),
            Some("e1".to_owned()),
            None,
            HashMap::new(),
        )
        .await
        .map(|_| ());
    assert!(
        matches!(result, Err(StreamableHttpError::SessionExpired)),
        "expected SessionExpired, got {result:?}"
    );
}

/// Without a session there is nothing to expire, so a 404 stays an ordinary error.
#[tokio::test]
async fn get_stream_404_without_session_is_not_session_expired() {
    let url = spawn_404_server().await;
    let result = reqwest::Client::new()
        .get_stream(Arc::from(url.as_str()), None, None, None, HashMap::new())
        .await
        .map(|_| ());
    assert!(result.is_err());
    assert!(!matches!(result, Err(StreamableHttpError::SessionExpired)));
}
