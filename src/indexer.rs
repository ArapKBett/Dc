use chrono::{DateTime, Utc, TimeZone};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
    commitment_config::CommitmentConfig,
};
use solana_transaction_status::{UiTransactionEncoding, EncodedConfirmedTransactionWithStatusMeta};
use std::str::FromStr;
use log::{info, warn, error};

#[derive(Debug)]
pub enum TransferType {
    Sent,
    Received,
}

#[derive(Debug)]
pub struct Transfer {
    pub signature: String,
    pub timestamp: DateTime<Utc>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub amount: f64,
    pub transfer_type: TransferType,
}

pub async fn index_usdc_transfers(
    client: &RpcClient,
    wallet: &str,
    usdc_mint: &str,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> Result<Vec<Transfer>, Box<dyn std::error::Error>> {
    let wallet_pubkey = Pubkey::from_str(wallet)?;
    let usdc_mint_pubkey = Pubkey::from_str(usdc_mint)?;

    info!("Fetching signatures for wallet: {}", wallet);
    let signatures = client
        .get_signatures_for_address_with_config(
            &wallet_pubkey,
            solana_client::rpc_config::RpcSignaturesForAddressConfig {
                before: None,
                until: None,
                limit: Some(5000),
                commitment: Some(CommitmentConfig::confirmed()),
                min_context_slot: None,
            },
        )
        .await
        .map_err(|e| {
            error!("Failed to get signatures: {}", e);
            Box::new(e) as Box<dyn std::error::Error>
        })?;

    info!("Found {} signatures", signatures.len());
    let mut transfers = Vec::new();

    for sig_info in signatures {
        let signature = Signature::from_str(&sig_info.signature)?;
        let block_time = sig_info
            .block_time
            .map(|t| Utc.timestamp_opt(t, 0).single().ok_or("Invalid timestamp"))
            .transpose()
            .map_err(|e| {
                error!("Invalid block time for signature {}: {}", signature, e);
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e))
            })?;

        if let Some(tx_time) = block_time {
            if tx_time < start_time || tx_time > end_time {
                info!(
                    "Skipping signature {}: timestamp {} outside range [{}, {}]",
                    signature, tx_time, start_time, end_time
                );
                continue;
            }

            info!("Fetching transaction for signature: {}", signature);
            let tx = client
                .get_transaction(&signature, UiTransactionEncoding::JsonParsed)
                .await
                .map_err(|e| {
                    error!("Failed to get transaction {}: {}", signature, e);
                    Box::new(e) as Box<dyn std::error::Error>
                })?;

            transfers.extend(process_transaction(&tx, &wallet_pubkey, &usdc_mint_pubkey, tx_time, &signature));
        } else {
            warn!("No block time for signature: {}", signature);
        }
    }

    info!("Returning {} transfers", transfers.len());
    Ok(transfers)
}

fn process_transaction(
    tx: &EncodedConfirmedTransactionWithStatusMeta,
    wallet_pubkey: &Pubkey,
    usdc_mint_pubkey: &Pubkey,
    tx_time: DateTime<Utc>,
    signature: &Signature,
) -> Vec<Transfer> {
    let mut transfers = Vec::new();

    if let Some(meta) = &tx.transaction.meta {
        let pre_balances = meta.pre_token_balances.as_ref().unwrap_or(&vec![]);
        let post_balances = meta.post_token_balances.as_ref().unwrap_or(&vec![]);

        for (pre, post) in pre_balances.iter().zip(post_balances.iter()) {
            // Check if token mint and owner match
            if pre.mint == *usdc_mint_pubkey && post.mint == *usdc_mint_pubkey {
                // Check that the wallet is one of the owners (pre or post) -- mostly pre.owner and post.owner are same
                if let (Some(pre_owner), Some(post_owner)) = (&pre.owner, &post.owner) {
                    if pre_owner != wallet_pubkey.to_string() && post_owner != wallet_pubkey.to_string() {
                        continue; // Not related to wallet, skip
                    }
                } else {
                    // Skip if no owner info
                    continue;
                }

                // Calculate amount change
                let pre_amount = pre.ui_token_amount.ui_amount.unwrap_or(0.0);
                let post_amount = post.ui_token_amount.ui_amount.unwrap_or(0.0);
                let diff = post_amount - pre_amount;

                if diff.abs() < f64::EPSILON {
                    continue; // No transfer amount change
                }

                let transfer_type = if diff > 0.0 {
                    TransferType::Received
                } else {
                    TransferType::Sent
                };

                let from = if transfer_type == TransferType::Sent {
                    Some(wallet_pubkey.to_string())
                } else {
                    None
                };

                let to = if transfer_type == TransferType::Received {
                    Some(wallet_pubkey.to_string())
                } else {
                    None
                };

                transfers.push(Transfer {
                    signature: signature.to_string(),
                    timestamp: tx_time,
                    from,
                    to,
                    amount: diff.abs(),
                    transfer_type,
                });
            }
        }
    }

    transfers
}
