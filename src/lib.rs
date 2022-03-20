use mongodb::{
    bson::doc,
    options::{ClientOptions, UpdateOptions},
    Client, Database,
};
use num_traits::cast::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::{
    error, panic,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{task, time::sleep, try_join};
use web3::{
    contract::{Contract, Options},
    ethabi::{decode, param_type::ParamType, Token},
    signing::keccak256,
    transports::Http,
    types::{Address, BlockNumber, FilterBuilder, Log, H160, H256, U64},
    Web3,
};

#[derive(Debug, Serialize, Deserialize)]
struct ContractAddress {
    address: H160,
    token_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct TokenOwnership {
    contract_address: H160,
    token_id: String,
    owner: H160,
    quantity: f64,
}

#[derive(Debug)]
struct ERC1155DecodedData {
    token_id: String,
    quantity: f64,
}

#[derive(Debug)]
pub struct Worker {
    database: Database,
    web3: Web3<Http>,
}

impl Worker {
    pub async fn new(
        database_host: String,
        database_name: String,
        ethereum_json_rpc_api_endpoint: String,
    ) -> Result<Self, Box<dyn error::Error>> {
        let database = get_database(database_host, database_name).await?;
        let web3 = get_web3_http(ethereum_json_rpc_api_endpoint).await?;

        database
            .run_command(
                doc! {
                    "ping": 1
                },
                None,
            )
            .await?;

        Ok(Self { database, web3 })
    }

    pub async fn start(self) {
        let latest_block = Arc::new(Mutex::new(None));

        let logs_worker_latest_block = latest_block.clone();

        let logs_worker_web3 = self.web3.clone();

        let latest_block_worker = task::spawn(async move {
            loop {
                *latest_block.lock().unwrap() = match self.web3.eth().block_number().await {
                    Ok(value) => Some(value),
                    Err(_) => {
                        eprintln!("Error: Could not get the current block number, retrying...");
                        continue;
                    }
                };
                sleep(Duration::from_millis(60000)).await;
            }
        });

        let logs_worker = task::spawn(async move {
            let contract_address_collection = self
                .database
                .collection::<ContractAddress>("contract_addresses");
            let token_ownership_collection = self
                .database
                .collection::<TokenOwnership>("token_ownerships");

            let mut current_block = U64::from(14282071 as u64);

            let erc_20_and_721_transfer_signature =
                H256::from(keccak256("Transfer(address,address,uint256)".as_bytes()));
            let erc_1155_transfer_single_signature = H256::from(keccak256(
                "TransferSingle(address,address,address,uint256,uint256)".as_bytes(),
            ));
            let erc_1155_transfer_batch_signature = H256::from(keccak256(
                "TransferBatch(address,address,address,uint256[],uint256[])".as_bytes(),
            ));

            loop {
                let latest_block = *logs_worker_latest_block.lock().unwrap();

                if let Some(latest_block) = latest_block {
                    if current_block <= latest_block {
                        println!(
                            "Processing block {} of {} blocks",
                            current_block, latest_block
                        );

                        let signatures_filter = vec![
                            erc_20_and_721_transfer_signature,
                            erc_1155_transfer_single_signature,
                            erc_1155_transfer_batch_signature,
                        ];

                        let filter = FilterBuilder::default()
                            .from_block(BlockNumber::Number(current_block))
                            .to_block(BlockNumber::Number(current_block))
                            .topics(Some(signatures_filter), None, None, None)
                            .build();

                        let logs: Vec<Log> = match logs_worker_web3.eth().logs(filter).await {
                            Ok(logs) => logs,
                            Err(error) => {
                                eprintln!("Error: Could not get the logs, retrying... {}", error);
                                continue;
                            }
                        };

                        for log in logs {
                            let contract = Contract::from_json(
                                logs_worker_web3.eth(),
                                log.address,
                                include_bytes!("supports_interface_abi.json"),
                            )
                            .unwrap();

                            let erc_721_interface_id: [u8; 4] =
                                hex::decode("80ac58cd").unwrap()[0..4].try_into().unwrap();

                            let erc_1155_interface_id: [u8; 4] =
                                hex::decode("d9b67a26").unwrap()[0..4].try_into().unwrap();

                            let mut token_type: Option<String> = None;

                            match contract_address_collection
                                .find_one(
                                    doc! {
                                        "address": format!("{:#x}", log.address),
                                    },
                                    None,
                                )
                                .await
                            {
                                Ok(contract_address) => {
                                    if let Some(contract_address) = contract_address {
                                        token_type = Some(contract_address.token_type)
                                    }
                                }
                                Err(_) => {
                                    panic!()
                                }
                            }

                            if let None = token_type {
                                token_type = if log.topics[0] == erc_20_and_721_transfer_signature
                                    && log.topics.len() == 3
                                {
                                    Some("ERC20".to_string())
                                } else if log.topics[0] == erc_20_and_721_transfer_signature
                                    && log.topics.len() == 4
                                {
                                    let supports_interface: bool = match contract
                                        .query(
                                            "supportsInterface",
                                            (erc_721_interface_id,),
                                            None,
                                            Options::default(),
                                            None,
                                        )
                                        .await
                                    {
                                        Ok(value) => value,
                                        Err(_) => {
                                            continue;
                                        }
                                    };

                                    if supports_interface {
                                        Some("ERC721".to_string())
                                    } else {
                                        None
                                    }
                                } else if log.topics[0] == erc_1155_transfer_single_signature
                                    || log.topics[0] == erc_1155_transfer_batch_signature
                                {
                                    let supports_interface: bool = match contract
                                        .query(
                                            "supportsInterface",
                                            (erc_1155_interface_id,),
                                            None,
                                            Options::default(),
                                            None,
                                        )
                                        .await
                                    {
                                        Ok(value) => value,
                                        Err(_) => {
                                            continue;
                                        }
                                    };

                                    if supports_interface {
                                        Some("ERC1155".to_string())
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                };
                            }

                            if let Some(token_type) = &token_type {
                                contract_address_collection
                                    .update_one(
                                        doc! {
                                            "address": format!("{:#x}", log.address),
                                        },
                                        doc! {
                                            "$set": {
                                                "token_type": token_type,
                                            }
                                        },
                                        UpdateOptions::builder().upsert(true).build(),
                                    )
                                    .await
                                    .unwrap();

                                if token_type == "ERC20" {
                                    if Address::from(log.topics[1]) != Address::default() {
                                        let decoded_quantity = match decode(
                                            &vec![ParamType::Uint(256)],
                                            &log.data.0,
                                        ) {
                                            Ok(decoded) => match decoded[0] {
                                                Token::Uint(decoded_quantity) => decoded_quantity,
                                                _ => panic!(),
                                            },
                                            Err(_) => panic!(),
                                        };

                                        let quantity = decoded_quantity.as_u128().to_f64().unwrap();

                                        if quantity > 0.0 {
                                            token_ownership_collection.update_one(
                                                doc! {
                                                    "contract_address": format!("{:#x}", log.address),
                                                    "owner": format!("{:#x}", Address::from(log.topics[1])),
                                                },
                                                doc! {
                                                    "$inc": {
                                                        "quantity": -quantity
                                                    }

                                                },
                                                UpdateOptions::builder().upsert(true).build(),
                                            ).await.unwrap();

                                            token_ownership_collection.update_one(
                                                doc! {
                                                    "contract_address": format!("{:#x}", log.address),
                                                    "owner": format!("{:#x}", Address::from(log.topics[2])),
                                                },
                                                doc! {
                                                    "$inc": {
                                                        "quantity": quantity
                                                    }

                                                },
                                                UpdateOptions::builder().upsert(true).build(),
                                            ).await.unwrap();
                                        }
                                    }
                                } else if token_type == "ERC721" {
                                    let decoded_token_id = match decode(
                                        &vec![ParamType::Uint(256)],
                                        &log.topics[3].as_bytes(),
                                    ) {
                                        Ok(decoded) => match decoded[0] {
                                            Token::Uint(token_id) => token_id,
                                            _ => panic!(),
                                        },
                                        Err(_) => panic!(),
                                    };

                                    let token_id = decoded_token_id.to_string();

                                    if Address::from(log.topics[1]) != Address::default()
                                        && Address::from(log.topics[2]) != Address::default()
                                    {
                                        token_ownership_collection.delete_many(doc! {
                                            "contract_address": format!("{:#x}", log.address),
                                            "owner": format!("{:#x}", Address::from(log.topics[1])),
                                            "token_id": &token_id
                                        }, None).await.unwrap();

                                        token_ownership_collection
                                            .insert_one(
                                                TokenOwnership {
                                                    contract_address: log.address,
                                                    token_id,
                                                    owner: Address::from(log.topics[2]),
                                                    quantity: 1.0,
                                                },
                                                None,
                                            )
                                            .await
                                            .unwrap();
                                    } else if Address::from(log.topics[2]) == Address::default() {
                                        token_ownership_collection
                                        .delete_many(
                                            doc! {
                                                "contract_address": format!("{:#x}", log.address),
                                                "token_id": token_id,
                                            },
                                            None,
                                        )
                                        .await.unwrap();
                                    }
                                } else if token_type == "ERC1155" {
                                    let mut transferred_tokens: Vec<ERC1155DecodedData> =
                                        Vec::new();

                                    if log.topics[0] == erc_1155_transfer_single_signature {
                                        match decode(
                                            &vec![ParamType::Uint(256), ParamType::Uint(256)],
                                            &log.data.0,
                                        ) {
                                            Ok(decoded) => {
                                                if let (
                                                    Token::Uint(token_id),
                                                    Token::Uint(quantity),
                                                ) =
                                                    (decoded[0].to_owned(), decoded[1].to_owned())
                                                {
                                                    transferred_tokens.push(ERC1155DecodedData {
                                                        token_id: token_id.to_string(),
                                                        quantity: quantity
                                                            .as_u128()
                                                            .to_f64()
                                                            .unwrap(),
                                                    })
                                                }
                                            }
                                            Err(_) => panic!(),
                                        };
                                    } else if log.topics[0] == erc_1155_transfer_batch_signature {
                                        match decode(
                                            &vec![
                                                ParamType::Array(Box::new(ParamType::Uint(256))),
                                                ParamType::Array(Box::new(ParamType::Uint(256))),
                                            ],
                                            &log.data.0,
                                        ) {
                                            Ok(decoded) => {
                                                if let (
                                                    Token::Array(token_ids),
                                                    Token::Array(quantities),
                                                ) =
                                                    (decoded[0].to_owned(), decoded[1].to_owned())
                                                {
                                                    for (index, _) in token_ids.iter().enumerate() {
                                                        if let (
                                                            Token::Uint(token_id),
                                                            Token::Uint(quantity),
                                                        ) = (
                                                            token_ids[index].to_owned(),
                                                            quantities[index].to_owned(),
                                                        ) {
                                                            transferred_tokens.push(
                                                                ERC1155DecodedData {
                                                                    token_id: token_id.to_string(),
                                                                    quantity: quantity
                                                                        .as_u128()
                                                                        .to_f64()
                                                                        .unwrap(),
                                                                },
                                                            )
                                                        }
                                                    }
                                                }
                                            }
                                            Err(_) => panic!(),
                                        };

                                        for transferred_token in transferred_tokens {
                                            if Address::from(log.topics[2]) != Address::default()
                                                && Address::from(log.topics[3])
                                                    != Address::default()
                                            {
                                                if transferred_token.quantity > 0.0 {
                                                    token_ownership_collection.update_one(
                                                            doc! {
                                                                "contract_address": format!("{:#x}", log.address),
                                                                "owner": format!("{:#x}", Address::from(log.topics[2])),
                                                                "token_id": &transferred_token.token_id,
                                                            },
                                                            doc! {
                                                                "$inc": {
                                                                    "quantity": -transferred_token.quantity
                                                            }
                                                        },
                                                            UpdateOptions::builder().upsert(true).build(),
                                                        ).await.unwrap();

                                                    token_ownership_collection.update_one(
                                                            doc! {
                                                                "contract_address": format!("{:#x}", log.address),
                                                                "owner": format!("{:#x}", Address::from(log.topics[3])),
                                                                "token_id": transferred_token.token_id,
                                                            },
                                                            doc! {
                                                                "$inc": {
                                                                    "quantity": transferred_token.quantity
                                                                }
                                                            },
                                                            UpdateOptions::builder().upsert(true).build(),
                                                        ).await.unwrap();
                                                }
                                            } else if Address::from(log.topics[3])
                                                == Address::default()
                                                && Address::from(log.topics[2])
                                                    != Address::default()
                                            {
                                                token_ownership_collection
                                                        .delete_many(
                                                        doc! {
                                                    "contract_address": format!("{:#x}", log.address),
                                                    "token_id": transferred_token.token_id,
                                                             },
                                                            None,
                                                        )
                                                        .await.unwrap();
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        current_block += U64::from(1 as u8);
                    } else {
                        println!("Waiting for new blocks");
                        sleep(Duration::from_millis(5000)).await;
                        continue;
                    }
                } else {
                    println!("Waiting for latest block");
                    sleep(Duration::from_millis(5000)).await;
                    continue;
                }
            }
        });

        match try_join!(latest_block_worker, logs_worker) {
            Ok(_) => {}
            Err(_) => eprintln!("Fatal Error: Worker stopped unexpectedly"),
        }
    }
}

async fn get_database(host: String, database: String) -> Result<Database, Box<dyn error::Error>> {
    let client_options = ClientOptions::parse(host).await?;

    let client = Client::with_options(client_options)?;

    let db = client.database(&database);

    Ok(db)
}

async fn get_web3_http(http_endpoint: String) -> Result<Web3<Http>, Box<dyn error::Error>> {
    let transport = Http::new(&http_endpoint)?;
    let web3 = Web3::new(transport);
    Ok(web3)
}
