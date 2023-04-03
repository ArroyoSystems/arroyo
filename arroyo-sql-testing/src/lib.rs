#[cfg(test)]
mod tests {
    use arroyo_sql_macro::single_test_codegen;

    // Casts
    single_test_codegen!(
        "cast_i64_f32",
        "CAST(nullable_i64 as FLOAT)",
        arroyo_sql::TestStruct {
            nullable_i64: Some(5),
            ..Default::default()
        },
        Some(5f32)
    );

    single_test_codegen!(
        "cast_null",
        "CAST(nullable_i64 as FLOAT)",
        arroyo_sql::TestStruct {
            nullable_i64: None,
            ..Default::default()
        },
        None
    );

    single_test_codegen!(
        "cast_string_to_f32",
        "CAST(nullable_string as FLOAT)",
        arroyo_sql::TestStruct {
            nullable_string: Some("1.25".to_string()),
            ..Default::default()
        },
        Some(1.25f32)
    );

    single_test_codegen!(
        "cast_f64_to_string",
        "CAST(nullable_f64 as STRING)",
        arroyo_sql::TestStruct {
            nullable_f64: Some(1.25),
            ..Default::default()
        },
        Some("1.25".to_string())
    );
    // Category: Math - Addition

    // Test case: Non-nullable and nullable values, nullable is non-null
    single_test_codegen!(
        "one_plus_nullable_two",
        "non_nullable_i32 + nullable_i64",
        arroyo_sql::TestStruct {
            non_nullable_i32: 1,
            nullable_i64: Some(2),
            ..Default::default()
        },
        Some(3i64)
    );

    // Test case: Non-nullable and nullable values, nullable is null
    single_test_codegen!(
        "one_plus_null",
        "non_nullable_i32 + nullable_i64",
        arroyo_sql::TestStruct {
            non_nullable_i32: 1,
            nullable_i64: None,
            ..Default::default()
        },
        None
    );

    // Test case: Nullable and non-nullable values, nullable is non-null
    single_test_codegen!(
        "nullable_two_plus_one",
        "nullable_i32 + non_nullable_i64",
        arroyo_sql::TestStruct {
            nullable_i32: Some(1),
            non_nullable_i64: 2,
            ..Default::default()
        },
        Some(3i64)
    );

    // Test case: Nullable and non-nullable values, nullable is null
    single_test_codegen!(
        "null_plus_two",
        "nullable_i32 + non_nullable_i64",
        arroyo_sql::TestStruct {
            nullable_i32: None,
            non_nullable_i64: 2,
            ..Default::default()
        },
        None
    );

    // Category: Math - Subtraction

    // Test case: Non-nullable and nullable values, nullable is non-null
    single_test_codegen!(
        "one_minus_nullable_two",
        "non_nullable_i32 - nullable_i64",
        arroyo_sql::TestStruct {
            non_nullable_i32: 1,
            nullable_i64: Some(2),
            ..Default::default()
        },
        Some(-1i64)
    );

    // Test case: Non-nullable and nullable values, nullable is null
    single_test_codegen!(
        "one_minus_null",
        "non_nullable_i32 - nullable_i64",
        arroyo_sql::TestStruct {
            non_nullable_i32: 1,
            nullable_i64: None,
            ..Default::default()
        },
        None
    );

    // Test case: Nullable and non-nullable values, nullable is non-null
    single_test_codegen!(
        "nullable_two_minus_one",
        "nullable_i32 - non_nullable_i64",
        arroyo_sql::TestStruct {
            nullable_i32: Some(2),
            non_nullable_i64: 1,
            ..Default::default()
        },
        Some(1i64)
    );

    // Test case: Nullable and non-nullable values, nullable is null
    single_test_codegen!(
        "null_minus_two",
        "nullable_i32 - non_nullable_i64",
        arroyo_sql::TestStruct {
            nullable_i32: None,
            non_nullable_i64: 2,
            ..Default::default()
        },
        None
    );
    // Category: Math - Modulo

    // Test case: Non-nullable and nullable values, nullable is non-null
    single_test_codegen!(
        "one_modulo_nullable_two",
        "non_nullable_i32 % nullable_i64",
        arroyo_sql::TestStruct {
            non_nullable_i32: 1,
            nullable_i64: Some(2),
            ..Default::default()
        },
        Some(1i64)
    );

    // Test case: Non-nullable and nullable values, nullable is null
    single_test_codegen!(
        "one_modulo_null",
        "non_nullable_i32 % nullable_i64",
        arroyo_sql::TestStruct {
            non_nullable_i32: 1,
            nullable_i64: None,
            ..Default::default()
        },
        None
    );

    // Test case: Nullable and non-nullable values, nullable is non-null
    single_test_codegen!(
        "nullable_two_modulo_one",
        "nullable_i32 % non_nullable_i64",
        arroyo_sql::TestStruct {
            nullable_i32: Some(2),
            non_nullable_i64: 1,
            ..Default::default()
        },
        Some(0i64)
    );

    // Test case: Nullable and non-nullable values, nullable is null
    single_test_codegen!(
        "null_modulo_two",
        "nullable_i32 % non_nullable_i64",
        arroyo_sql::TestStruct {
            nullable_i32: None,
            non_nullable_i64: 2,
            ..Default::default()
        },
        None
    );
    // Category: Unary Operators

    // IS NOT NULL
    single_test_codegen!(
        "non_nullable_bool_is_not_null",
        "non_nullable_bool IS NOT NULL",
        arroyo_sql::TestStruct {
            non_nullable_bool: true,
            ..Default::default()
        },
        true
    );

    single_test_codegen!(
        "nullable_bool_is_not_null",
        "nullable_bool IS NOT NULL",
        arroyo_sql::TestStruct {
            nullable_bool: Some(true),
            ..Default::default()
        },
        true
    );

    // IS NULL
    single_test_codegen!(
        "nullable_bool_is_null",
        "nullable_bool IS NULL",
        arroyo_sql::TestStruct {
            nullable_bool: None,
            ..Default::default()
        },
        true
    );

    // IS TRUE
    single_test_codegen!(
        "non_nullable_bool_is_true",
        "non_nullable_bool IS TRUE",
        arroyo_sql::TestStruct {
            non_nullable_bool: true,
            ..Default::default()
        },
        true
    );

    single_test_codegen!(
        "nullable_bool_is_true",
        "nullable_bool IS TRUE",
        arroyo_sql::TestStruct {
            nullable_bool: Some(true),
            ..Default::default()
        },
        true
    );

    // IS FALSE
    single_test_codegen!(
        "non_nullable_bool_is_false",
        "non_nullable_bool IS FALSE",
        arroyo_sql::TestStruct {
            non_nullable_bool: false,
            ..Default::default()
        },
        true
    );

    single_test_codegen!(
        "nullable_bool_is_false",
        "nullable_bool IS FALSE",
        arroyo_sql::TestStruct {
            nullable_bool: Some(false),
            ..Default::default()
        },
        true
    );

    // IS UNKNOWN
    single_test_codegen!(
        "nullable_bool_is_unknown",
        "nullable_bool IS UNKNOWN",
        arroyo_sql::TestStruct {
            nullable_bool: None,
            ..Default::default()
        },
        true
    );

    // IS NOT TRUE
    single_test_codegen!(
        "non_nullable_bool_is_not_true",
        "non_nullable_bool IS NOT TRUE",
        arroyo_sql::TestStruct {
            non_nullable_bool: false,
            ..Default::default()
        },
        true
    );

    single_test_codegen!(
        "nullable_bool_is_not_true",
        "nullable_bool IS NOT TRUE",
        arroyo_sql::TestStruct {
            nullable_bool: Some(false),
            ..Default::default()
        },
        true
    );

    // IS NOT FALSE
    single_test_codegen!(
        "non_nullable_bool_is_not_false",
        "non_nullable_bool IS NOT FALSE",
        arroyo_sql::TestStruct {
            non_nullable_bool: true,
            ..Default::default()
        },
        true
    );

    single_test_codegen!(
        "nullable_bool_is_not_false",
        "nullable_bool IS NOT FALSE",
        arroyo_sql::TestStruct {
            nullable_bool: Some(true),
            ..Default::default()
        },
        true
    );
}
