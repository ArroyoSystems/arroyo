use anyhow::anyhow;
use arroyo_rpc::grpc::api::CreateConnectionTableReq;
use deadpool_postgres::Pool;
use tonic::Status;

use crate::{AuthData, log_and_map, queries::api_queries, connectors, handle_db_error};



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

    let connection_id: i64 = req.connection_id.parse()
        .map_err(|_| Status::failed_precondition(format!("No connection with id '{}'", req.connection_id)))?;

    let connection = api_queries::get_connection_by_id()
        .bind(&transaction, &auth.organization_id, &connection_id)
        .opt()
        .await
        .map_err(log_and_map)?
        .ok_or_else(|| Status::failed_precondition(format!("No connection with id '{}'", req.connection_id)))?;

    {
        let connector =
            connectors::connector_for_type(&connection.r#type)
            .ok_or_else(|| anyhow!("connector not found for stored connection with type '{}'", connection.r#type))
            .map_err(log_and_map)?;

        connector.validate_table(&req.config)
            .map_err(|e| Status::invalid_argument(&format!("Failed to parse config: {:?}", e)))?;
    }

    let _: connectors::Schema = serde_json::from_str(&req.schema)
        .map_err(|e| Status::invalid_argument(&format!("Failed to parse schema: {:?}", e)))?;

    api_queries::create_connection_table()
        .bind(
            &transaction,
            &auth.organization_id,
            &auth.user_id,
            &req.name,
            &req.r#type().as_str_name(),
            &connection_id,
            &serde_json::to_value(&req.config).unwrap(),
            &serde_json::to_value(&req.schema).unwrap(),
        )
        .await
        .map_err(|err| handle_db_error("connection_table", err))?;

    transaction.commit().await.map_err(log_and_map)?;
    Ok(())
}
