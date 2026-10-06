use super::*;
use axum::{Json, Router, extract::Query, routing::get};

#[tokio::test]
async fn follows_continuation_even_when_page_is_smaller_than_requested() {
    let app = Router::new().route("/tdata/Insights", get(|Query(query): Query<BTreeMap<String, String>>| async move {
        if query.contains_key("$skiptoken") {
            Json(serde_json::json!({"value": [{"entity_id": "bad", "status": "Removed"}]}))
        } else {
            Json(serde_json::json!({"value": [{"entity_id": "good", "status": "Pending"}], "@odata.nextLink": "Insights?$skiptoken=page2"}))
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let rows = fetch_rows(&reqwest::Client::new(), &base, "Insights", None, None)
        .await
        .unwrap()
        .unwrap();
    server.abort();
    assert_eq!(rows.len(), 2, "the second page must also be audited");
    assert_eq!(rows[1]["entity_id"], "bad");
}

#[tokio::test]
async fn incomplete_or_unsafe_continuations_fail_the_audit() {
    use axum::{extract::Path, http::StatusCode, response::IntoResponse};
    let app = Router::new().route(
        "/tdata/{set}",
        get(
            |Path(set): Path<String>, Query(query): Query<BTreeMap<String, String>>| async move {
                if set == "Denied" && query.contains_key("$skiptoken") {
                    return StatusCode::FORBIDDEN.into_response();
                }
                let next = match set.as_str() {
                    "Cycle" => "Cycle?$skiptoken=same",
                    "Foreign" => "http://example.invalid/tdata/Foreign",
                    _ => "Denied?$skiptoken=page2",
                };
                Json(serde_json::json!({"value": [], "@odata.nextLink": next})).into_response()
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    for (set, message) in [
        ("Cycle", "repeated"),
        ("Foreign", "cross-origin"),
        ("Denied", "incomplete"),
    ] {
        let error = fetch_rows(
            &reqwest::Client::new(),
            &base,
            set,
            Some("test-token"),
            Some("test-tenant"),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
    }
    server.abort();
}
