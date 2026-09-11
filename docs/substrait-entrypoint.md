# Substrait Entrypoint

## Goal

Add a Substrait-based planning entrypoint to Arroyo, allowing callers to submit a Substrait logical plan instead of SQL.

The Substrait entrypoint should reuse Arroyo’s existing DataFusion-based planning infrastructure rather than introduce a separate planning pipeline.

## Proposed Planning Flow

1. Decode a Substrait protobuf plan.
2. Convert it into a DataFusion `LogicalPlan`.
3. Resolve named tables through Arroyo’s schema and connector catalog.
4. Apply Arroyo’s existing streaming-specific logical plan rewrites.
5. Lower the rewritten plan into an Arroyo `LogicalProgram`.

This creates two frontends that share the same downstream planner:

- SQL → DataFusion `LogicalPlan`
- Substrait → DataFusion `LogicalPlan`

## API Shape

A Substrait plan does not contain all the configuration required to run an Arroyo pipeline. Source connectors, sink connectors, event-time configuration, and watermark behavior therefore need to be supplied separately.

An initial API could accept an envelope containing:

- A serialized Substrait plan
- Source definitions
- A sink definition
- Planning options

Protobuf binary should be the canonical plan encoding. JSON may be supported as a convenience format.

## Implementation Approach

Refactor the existing SQL planner so that the conversion from a DataFusion `LogicalPlan` into an Arroyo `LogicalProgram` is available as a shared function.

The SQL and Substrait entrypoints would then be responsible only for producing the initial DataFusion logical plan and assembling the required catalog information.

The Substrait frontend would use DataFusion’s Substrait consumer to perform the initial conversion.

## Initial Scope

The first version should:

- Accept one Substrait root relation
- Resolve named tables against Arroyo’s catalog
- Support common relational operators such as projections, filters, joins, and aggregates
- Reuse Arroyo’s existing validation and streaming rewrites
- Return clear errors for unsupported relations, expressions, and functions

## Compatibility Considerations

Arroyo currently uses a patched DataFusion dependency. The DataFusion Substrait crate must be built against the same DataFusion version and source to avoid incompatible Rust types.

Substrait support in DataFusion is still evolving. Compatibility should be verified with focused round-trip and cross-producer tests rather than assuming that every valid Substrait plan can be consumed.

Function extension URIs and names also need an explicit compatibility policy, particularly for Arroyo-specific functions and streaming window behavior.

## Open Questions

- What external API should expose Substrait plan submission?
- How should sources and sinks be associated with named tables?
- Should the initial API accept protobuf only, or protobuf and JSON?
- Which Substrait producers should be tested for compatibility?
- How should Arroyo-specific functions be represented?
- Should event-time and watermark configuration remain entirely outside the Substrait plan?
