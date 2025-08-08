use chrono::{DateTime, Utc, TimeZone};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
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
            solana_client::rpc_config::RpcGetConfirmedSignaturesForAddress2Config {
                before: None,
                until: None,
                limit: Some(5000), // Increased limit for high transaction volume
                commitment: Some(solana_sdk::commitment_config::CommitmentConfig::confirmed()),
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
        let post_balances = meta.post_token_balances.as_ref().unwrap_or(&vec![]);
        info!("Signature {}: Found {} pre_balances, {} post_balances", 
            signature, pre_balances.len(), post_balances.len());
        
        // Check if wallet is a signer
        let is_signer = tx.transaction.transaction.message().account_keys.iter().any(|key| key.pubkey == *wallet_pubkey);
        info!("Signature {}: Wallet {} is_signer: {}", signature, wallet_pubkey, is_signer);
        
        // Process token balances
        for post_balance in post_balances {
            if post_balance.mint != usdc_mint_pubkey.to_string() {
                continue;
            }
            
            let pre_balance = pre_balances.iter().find(|pre| {
                pre.account_index == post_balance.account_index && pre.mint == post_balance.mint
            });
            
            let pre_amount = pre_balance
                .map(|pre| pre.ui_token_amount.ui_amount.unwrap_or(0.0))
                .unwrap_or(0.0);
            let post_amount = post_balance.ui_token_amount.ui_amount.unwrap_or(0.0);
            info!("Signature {}: USDC account_index {}: pre_amount: {}, post_amount: {}", 
                signature, post_balance.account_index, pre_amount, post_amount);
            
            if pre_amount != post_amount {
                let amount = (post_amount - pre_amount).abs();
                let transfer_type = if post_amount > pre_amount {
                    TransferType::Received
                } else {
                    TransferType::Sent
                };
                
                // Include transfer if wallet is a signer or involved in balance change
                if is_signer || post_balance.owner == wallet_pubkey.to_string() {
                    info!("Found transfer: {} USDC, type: {:?}", amount, transfer_type);
                    transfers.push(Transfer {
                        date: tx_time,
                        amount,
                        transfer_type,
                        signature: signature.to_string(),
                    });
                } else {
                    info!("Signature {}: Skipping transfer, wallet {} not owner ({}) or signer", 
                        signature, wallet_pubkey, post_balance.owner);
                }
            } else {
                info!("Signature {}: No USDC balance change for account_index {} (pre: {}, post: {})", 
                    signature, post_balance.account_index, pre_amount, post_amount);
            }
        }
    } else {
        warn!("No meta data for signature: {}", signature);
    }
    
    transfers
          }
