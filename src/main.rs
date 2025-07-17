use actix_web::{web, App, HttpServer, HttpResponse, Responder,  error::InternalError};
use serde::Deserialize;
use rusqlite::{params, Connection};
use std::sync::Mutex;

#[derive(Debug, Deserialize)]
struct LogEntry {
    client: String,
    request: String,
    status: i32,
    size: usize,
    #[serde(default)]
    referer: Option<String>,
    #[serde(default)]
    agent: Option<String>,
    timestamp: String,
    #[serde(default)]
    source_type: Option<String>,
    #[serde(default)]
    epoch: Option<i64>,
    instance: String,
}

#[derive(Debug, Deserialize)]
struct BackendLog {
    epoch: i64,
    instance: String,
    level: String,
    message: String,
    source: String,
    timestamp: String,
}


struct AppState {
    db: Mutex<Connection>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Running version 0.2.4 of http-to-sqlite-rs");
    let conn = Connection::open("logs.db").expect("Failed to open database");
    conn.execute(
        "CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client TEXT NOT NULL,
            request TEXT NOT NULL,
            status INTEGER NOT NULL,
            size INTEGER NOT NULL,
            referer TEXT,
            agent TEXT,
            timestamp TEXT NOT NULL,
            source_type TEXT NOT NULL,
            epoch INTEGER NOT NULL,
            instance TEXT NOT NULL
        )",
        [],
    ).expect("Failed to create table");

    conn.execute(
        "CREATE TABLE IF NOT EXISTS backend_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            epoch INTEGER NOT NULL,
            instance TEXT NOT NULL,
            level TEXT NOT NULL,
            message TEXT NOT NULL,
            source TEXT NOT NULL,
            timestamp TEXT NOT NULL
        )",
        [],
    ).expect("Failed to create backend_logs table");


    let shared_data = web::Data::new(AppState {
        db: Mutex::new(conn),
    });

    HttpServer::new(move || {
        App::new()
            // Override default Json extractor to catch errors
            .app_data(
                web::JsonConfig::default()
                    .error_handler(|err, _req| {
                        let err_text = err.to_string();  // Convert to String once
                        println!("JSON deserialization failed: {}", err_text);
                        // Return a descriptive 400 response
                        InternalError::from_response(
                            err,
                            HttpResponse::BadRequest()
                                .content_type("text/plain")
                                .body(format!("JSON error: {}", err_text))
                        )
                        .into()
                    })
            )
            .app_data(shared_data.clone())
            .route("/health", web::get().to(health_check))
            .route("/log/v1", web::post().to(receive_log))
            .route("/log/v1/backend", web::post().to(receive_backend_log))

    })
    .bind(("0.0.0.0", 6000))?
    .run()
    .await
}

async fn health_check() -> impl Responder {
    HttpResponse::Ok().body("OK")
}

async fn receive_log(data: web::Data<AppState>, json: web::Json<LogEntry>) -> impl Responder {
    let log = json.into_inner();
    let conn = data.db.lock().unwrap();

    let result = conn.execute(
        "INSERT INTO logs (client, request, status, size, referer, agent, timestamp, source_type, epoch, instance)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            log.client,
            log.request,
            log.status,
            log.size,
            log.referer.unwrap_or_else(|| "-".to_string()),
            log.agent.unwrap_or_else(|| "-".to_string()),
            log.timestamp,
            log.source_type.unwrap_or_else(|| "-".to_string()),
            log.epoch.unwrap_or(0),
            log.instance,
        ],
    );

    match result {
        Ok(_) => HttpResponse::Ok().body("Log saved"),
        Err(e) => {
            println!("Database error: {}", e);
            HttpResponse::InternalServerError().body(format!("DB error: {}", e))
        },
    }
}

async fn receive_backend_log(data: web::Data<AppState>, json: web::Json<BackendLog>) -> impl Responder {
    let log = json.into_inner();
    let conn = data.db.lock().unwrap();

    let result = conn.execute(
        "INSERT INTO backend_logs (epoch, instance, level, message, source, timestamp)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            log.epoch,
            log.instance,
            log.level,
            log.message,
            log.source,
            log.timestamp,
        ],
    );

    match result {
        Ok(_) => HttpResponse::Ok().body("Backend log saved"),
        Err(e) => {
            println!("DB error: {}", e);
            HttpResponse::InternalServerError().body(format!("DB error: {}", e))
        },
    }
}


// async fn debug_log(body: web::Bytes) -> HttpResponse {
//     // Print raw payload for inspection
//     match std::str::from_utf8(&body) {
//         Ok(txt) => println!("Raw body from Vector: {}", txt),
//         Err(e) => println!("Non-UTF8 body: {:?}, error: {}", &body, e),
//     }
//     HttpResponse::Ok().body("debug")
// }