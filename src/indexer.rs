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

use crate::models::{Transfer, TransferType};

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
                limit: Some(5000), // Increased limit for high transaction volume
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
                info!("Skipping signature {}: timestamp {} outside range [{}, {}]", 
                    signature, tx_time, start_time, end_time);
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