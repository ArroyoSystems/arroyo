use cornucopia_async::GenericClient;
use deadpool_postgres::Pool;
use tonic::Status;

use arroyo_rpc::grpc::api::{create_sink_req, sink, CreateSinkReq, DeleteSinkReq, Sink};

use crate::handle_delete;
use crate::types::public;
use crate::types::public::SinkType;
use crate::{handle_db_error, log_and_map, queries::api_queries, required_field, AuthData};

pub(crate) async fn create_sink(
    req: CreateSinkReq,
    auth: AuthData,
    pool: &Pool,
) -> Result<(), Status> {
    let c = pool.get().await.map_err(log_and_map)?;

    let connections = api_queries::get_connections()
        .bind(&c, &auth.organization_id)
        .all()
        .await
        .map_err(log_and_map)?;

    let (source_type, config, connection_id) =
        match req.sink_type.ok_or_else(|| required_field("type"))? {
            create_sink_req::SinkType::Kafka(kafka) => {
                let connection = connections
                    .iter()
                    .find(|c| c.name == kafka.connection)
                    .ok_or_else(|| {
                        Status::failed_precondition(format!(
                            "Could not find connection with name '{}'",
                            kafka.connection
                        ))
                    })?;

                if connection.r#type != public::ConnectionType::kafka {
                    return Err(Status::invalid_argument(format!(
                        "Connection '{}' is not a kafka cluster",
                        kafka.connection
                    )));
                }

                (
                    SinkType::kafka,
                    serde_json::to_value(&kafka).unwrap(),
                    Some(connection.id),
                )
            }
        };

    api_queries::create_sink()
        .bind(
            &c,
            &auth.organization_id,
            &auth.user_id,
            &req.name,
            &source_type,
            &connection_id,
            &config,
        )
        .await
        .map_err(|err| handle_db_error("sink", err))?;

    Ok(())
}

pub(crate) async fn get_sinks<E: GenericClient>(
    auth: &AuthData,
    client: &E,
) -> Result<Vec<Sink>, Status> {
    let sinks = api_queries::get_sinks()
        .bind(client, &auth.organization_id)
        .all()
        .await
        .map_err(log_and_map)?;

    Ok(sinks
        .into_iter()
        .map(|sink| Sink {
            id: sink.id,
            name: sink.name,
            sink_type: Some(match sink.r#type {
                SinkType::kafka => {
                    sink::SinkType::Kafka(serde_json::from_value(sink.config).unwrap())
                }
                SinkType::state => todo!(),
            }),
            producers: sink.producers as i32,
        })
        .collect())
}

pub(crate) async fn delete_sink(
    req: DeleteSinkReq,
    auth: AuthData,
    client: &impl GenericClient,
) -> Result<(), Status> {
    let deleted = api_queries::delete_sink()
        .bind(client, &auth.organization_id, &req.name)
        .await
        .map_err(|e| handle_delete("sink", "pipelines", e))?;

    if deleted == 0 {
        return Err(Status::not_found(format!(
            "Not sink with name {}",
            req.name
        )));
    }

    Ok(())
}
