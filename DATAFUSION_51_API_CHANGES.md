# DataFusion 51 API Changes

This document tracks breaking API changes when upgrading from DataFusion 48 to DataFusion 51 (required for Arrow 57).

## 1. `ExecutionPlan::reset()` removed

**Old API:**
```rust
execution_plan.reset()?;
let stream = execution_plan.execute(0, ctx)?;
```

**New API:**
```rust
execution_plan = execution_plan.clone().reset_state()?;
let stream = execution_plan.execute(0, ctx)?;
```

The new `reset_state()` method consumes `Arc<Self>` and returns a new `Arc<dyn ExecutionPlan>`.

## 2. `parse_physical_expr` signature changed

**Old API:**
```rust
parse_physical_expr(
    &expr,
    registry.as_ref(),  // &dyn FunctionRegistry
    &schema,
    &codec,
)?
```

**New API:**
```rust
let task_context = SessionContext::new().task_ctx();
parse_physical_expr(
    &expr,
    &task_context,  // &TaskContext
    &schema,
    &codec,
)?
```

## 3. `try_into_physical_plan` signature changed

**Old API (3 args):**
```rust
plan.try_into_physical_plan(
    registry.as_ref(),
    &RuntimeEnvBuilder::new().build()?,
    &codec,
)?
```

**New API (2 args):**
```rust
let task_context = SessionContext::new().task_ctx();
plan.try_into_physical_plan(
    &task_context,
    &codec,
)?
```

The `RuntimeEnv` is now obtained from `TaskContext` internally.

## 4. `Accumulator::evaluate_mut()` removed

**Old API:**
```rust
accumulator.evaluate_mut()
```

**New API:**
```rust
accumulator.evaluate()
```

## 5. `AggregateFunctionExpr::sliding_state_fields()` removed

**Old API:**
```rust
AccumulatorType::Sliding => agg.sliding_state_fields()?
```

**New API:**
```rust
AccumulatorType::Sliding => agg.state_fields()?
```

## Files Modified

### arroyo-worker/src/arrow/mod.rs
- Fixed `StatelessPhysicalExecutor` to use `reset_state()`
- Fixed `ProjectionConstructor` to use 2-arg `try_into_physical_plan`
- Fixed `decode_aggregate` to take `&TaskContext` instead of `&dyn FunctionRegistry`

### arroyo-worker/src/arrow/async_udf.rs
- Changed struct field from `registry: Arc<Registry>` to `task_context: Arc<TaskContext>`
- Updated `parse_physical_expr` calls

### arroyo-worker/src/arrow/incremental_aggregator.rs
- Fixed `evaluate_mut()` to `evaluate()`
- Fixed `sliding_state_fields()` to `state_fields()`
- Fixed `parse_physical_expr` to use `&TaskContext`

### arroyo-worker/src/arrow/instant_join.rs
- Added `task_context: Arc<TaskContext>` field
- Fixed `try_into_physical_plan` to 2-arg version
- Fixed `reset()` to `reset_state()`

### arroyo-worker/src/arrow/join_with_expiration.rs
- Added `task_context: Arc<TaskContext>` field
- Fixed `try_into_physical_plan` to 2-arg version
- Fixed `reset()` to `reset_state()`

### arroyo-worker/src/arrow/tumbling_aggregating_window.rs
- Added `task_context: Arc<TaskContext>` field
- Fixed all `try_into_physical_plan` calls
- Fixed all `reset()` calls to `reset_state()`

### arroyo-worker/src/arrow/sliding_aggregating_window.rs
- Added `task_context: Arc<TaskContext>` field
- Fixed all `try_into_physical_plan` calls
- Fixed all `reset()` calls to `reset_state()`

### arroyo-worker/src/arrow/session_aggregating_window.rs
- Fixed `reset()` to `reset_state()` in `ActiveSession::new`
- Fixed `try_into_physical_plan` to 2-arg version

### arroyo-worker/src/arrow/watermark_generator.rs
- Fixed `parse_physical_expr` to use `&TaskContext`

### arroyo-worker/src/arrow/lookup_join.rs
- Fixed `parse_physical_expr` to use `&TaskContext`

### arroyo-worker/src/arrow/window_fn.rs
- Fixed `try_into_physical_plan` to 2-arg version
- Removed `RuntimeEnvBuilder` import
