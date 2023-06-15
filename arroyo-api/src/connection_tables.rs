use anyhow::anyhow;
use arroyo_rpc::grpc::api::{ConnectionTable, CreateConnectionTableReq, TableType};
use cornucopia_async::GenericClient;
use deadpool_postgres::Pool;
use tonic::Status;

use crate::{
    connectors, handle_db_error, log_and_map, queries::api_queries, AuthData,
};

pub(crate) async fn create(
    req: CreateConnectionTableReq,
    auth: AuthData,
    pool: &Pool,
) -> Result<(), Status> {
    let mut c = pool.get().await.map_err(log_and_map)?;

    let transaction = c.transaction().await.map_err(log_and_map)?;
    transaction
        .execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
        .await
        .map_err(log_and_map)?;

    let connection_id =
        if let Some(connection_id) = &req.connection_id {
            let connection_id: i64 = connection_id.parse().map_err(|_| {
                Status::failed_precondition(format!("No connection with id '{}'", connection_id))
            })?;

            let connection = api_queries::get_connection_by_id()
                .bind(&transaction, &auth.organization_id, &connection_id)
                .opt()
                .await
                .map_err(log_and_map)?
                .ok_or_else(|| {
                    Status::failed_precondition(format!("No connection with id '{}'", connection_id))
                })?;

            {
                let connector = connectors::connector_for_type(&connection.r#type)
                    .ok_or_else(|| {
                        anyhow!(
                            "connector not found for stored connection with type '{}'",
                            connection.r#type
                        )
                    })
                    .map_err(log_and_map)?;

                connector
                    .validate_table(&req.config)
                    .map_err(|e| Status::invalid_argument(&format!("Failed to parse config: {:?}", e)))?;
            };
            Some(connection_id)
        } else {
            None
        };

    let config: serde_json::Value = serde_json::from_str(&req.config).unwrap();
    let schema: Option<serde_json::Value> = req
        .schema
        .as_ref()
        .map(|s| serde_json::to_value(s).unwrap());

    api_queries::create_connection_table()
        .bind(
            &transaction,
            &auth.organization_id,
            &auth.user_id,
            &req.name,
            &req.r#type().as_str_name(),
            &connection_id,
            &config,
            &schema,
        )
        .await
        .map_err(|err| handle_db_error("connection_table", err))?;

    transaction.commit().await.map_err(log_and_map)?;
    Ok(())
}

pub(crate) async fn get<C: GenericClient>(
    auth: &AuthData,
    client: &C,
) -> Result<Vec<ConnectionTable>, Status> {
    let tables = api_queries::get_connection_tables()
        .bind(client, &auth.organization_id)
        .all()
        .await
        .map_err(log_and_map)?;

    Ok(tables
        .into_iter()
        .map(|t| ConnectionTable {
            name: t.name,
            connection_id: format!("{}", t.connection_id),
            connection_name: t.connection_name,
            connection_type: t.connection_type,
            connection_config: serde_json::to_string(&t.connection_config).unwrap(),
            r#type: TableType::from_str_name(&t.r#type).unwrap() as i32,
            config: serde_json::to_string(&t.config).unwrap(),
            schema: t.schema.map(|v| serde_json::from_value(v).unwrap()),
        })
        .collect())
}
