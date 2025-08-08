use actix_web::{App, HttpServer, HttpResponse, Responder, web as actix_web};
use chrono::{Duration, Utc};
use solana_client::nonblocking::rpc_client::RpcClient;
use std::env;
use log::{error, info};

mod indexer;
mod models;
mod web;

use indexer::index_usdc_transfers;
use web::get_transfers;

async fn root() -> impl Responder {
    HttpResponse::TemporaryRedirect()
        .append_header(("Location", "/transfers"))
        .finish()
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    
    let rpc_url = env::var("SOLANA_RPC_URL").unwrap_or("https://api.mainnet-beta.solana.com".to_string());
    info!("Using RPC URL: {}", rpc_url);
    let client = RpcClient::new(rpc_url);
    
    let wallet = "7cMEhpt9y3inBNVv8fNnuaEbx7hKHZnLvR1KWKKxuDDU".to_string();
    let usdc_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string();
    
    let end_time = Utc::now();
    let start_time = end_time - Duration::hours(96); // 96 hours per user change
    
    let transfers = match index_usdc_transfers(&client, &wallet, &usdc_mint, start_time, end_time).await {
        Ok(transfers) => {
            info!("Successfully indexed {} transfers", transfers.len());
            transfers
        }
        Err(e) => {
            error!("Failed to index transfers: {}", e);
            vec![]
        }
    };
    
    HttpServer::new(move || {
        App::new()
            .app_data(actix_web::Data::new(transfers.clone()))
            .route("/", actix_web::get().to(root))
            .route("/transfers", actix_web::get().to(get_transfers))
    })
    .bind(("0.0.0.0", 8080))?
    .workers(4)
    .run()
    .await
  }
