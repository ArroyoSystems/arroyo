use arroyo_sql::get_test_expression;
use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{parse_macro_input, Expr, LitStr, Token};

#[proc_macro]
pub fn single_test_codegen(input: TokenStream) -> TokenStream {
    let test_case = parse_macro_input!(input as TestCase);

    let test_name = &test_case.test_name;
    let calculation_string = &test_case.calculation_string;
    let input_value = &test_case.input_value;
    let expected_result = &test_case.expected_result;

    let function = get_test_expression(
        &test_name.value(),
        &calculation_string.value(),
        input_value,
        expected_result,
    );

    let tokens = quote! {
        #[test]
        #function
    };

    tokens.into()
}

struct TestCase {
    test_name: LitStr,
    calculation_string: LitStr,
    input_value: Expr,
    expected_result: Expr,
}

impl Parse for TestCase {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let test_name = input.parse()?;
        input.parse::<Token![,]>()?;
        let calculation_string = input.parse()?;
        input.parse::<Token![,]>()?;
        let input_value = input.parse()?;
        input.parse::<Token![,]>()?;
        let expected_result = input.parse()?;

        Ok(Self {
            test_name,
            calculation_string,
            input_value,
            expected_result,
        })
    }
}
