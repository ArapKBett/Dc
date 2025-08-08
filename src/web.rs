use actix_web::{web, HttpResponse, Responder};
use serde::Serialize;

use crate::models::{Transfer, TransferType};

#[derive(Serialize)]
struct TransferResponse {
    transfers: Vec<Transfer>,
}

pub async fn get_transfers(transfers: web::Data<Vec<Transfer>>) -> impl Responder {
    let table_content = if transfers.is_empty() {
        "<tr><td colspan=\"4\">No USDC transfers found in the last 96 hours. Check logs for RPC or parsing errors.</td></tr>".to_string()
    } else {
        transfers
            .iter()
            .map(|t| {
                let type_str = match t.transfer_type {
                    TransferType::Sent => "Sent".to_string(),
                    TransferType::Received => "Received".to_string(),
                };
                format!(
                    "<tr><td>{}</td><td>{:.6}</td><td>{}</td><td><a href=\"https://explorer.solana.com/tx/{}\">{}</a></td></tr>",
                    t.date, t.amount, type_str, t.signature, t.signature
                )
            })
            .collect::<Vec<_>>()
            .join("")
    };

    let html = format!(
        r#"
        <!DOCTYPE html>
        <html>
        <head>
            <title>USDC Transfers</title>
            <style>
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid black; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <h1>USDC Transfers for Wallet 7cMEhpt...xuDDU</h1>
            <table>
                <tr>
                    <th>Date</th>
                    <th>Amount (USDC)</th>
                    <th>Type</th>
                    <th>Signature</th>
                </tr>
                {}
            </table>
        </body>
        </html>
        "#,
        table_content
    );
    HttpResponse::Ok().content_type("text/html").body(html)
                      }
