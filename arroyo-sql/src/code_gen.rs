use arrow_schema::DataType;
use quote::quote;
use quote::ToTokens;
use syn::parse_quote;

use crate::types::{data_type_as_syn_type, StructDef, TypeDef};

pub trait CodeGenerator<Context, OutputValue, OutputType: ToTokens> {
    // Generate a syn::Expr based on the input context
    fn generate(&self, input_context: &Context) -> OutputType;
    fn expression_type(&self, input_context: &Context) -> OutputValue;
}

impl Typed for () {
    fn syn_type(&self) -> syn::Type {
        parse_quote!(())
    }
}

pub trait Typed {
    fn syn_type(&self) -> syn::Type;
}

pub struct JoinPairContext {
    pub left_struct: StructDef,
    pub right_struct: StructDef,
}

impl JoinPairContext {
    pub fn new(left_struct: StructDef, right_struct: StructDef) -> Self {
        JoinPairContext {
            left_struct,
            right_struct,
        }
    }

    pub fn left_ident(&self) -> syn::Ident {
        parse_quote!(left)
    }

    pub fn right_ident(&self) -> syn::Ident {
        parse_quote!(right)
    }

    pub(crate) fn compile_pair_merge_record_expression<
        CG: CodeGenerator<Self, StructDef, syn::Expr>,
    >(
        &self,
        code_generator: &CG,
    ) -> syn::Expr {
        let merge_expr = code_generator.generate(self);
        let left_ident = self.left_ident();
        let right_ident = self.right_ident();
        parse_quote!({
            let #left_ident = &record.value.0;
            let #right_ident = &record.value.1;
            arroyo_types::Record {
                timestamp: record.timestamp.clone(),
                key: None,
                value: #merge_expr
            }
        })
    }

    pub fn compile_updating_pair_merge_value_expression<
        CG: CodeGenerator<Self, StructDef, syn::Expr>,
    >(
        &self,
        code_generator: &CG,
    ) -> syn::Expr {
        let merge_expr = code_generator.generate(self);
        let left_ident = self.left_ident();
        let right_ident = self.right_ident();
        parse_quote!({
            let #left_ident = &arg.0;
            let #right_ident = &arg.1;
            Some(#merge_expr)
        })
    }
}

pub struct ValuePointerContext;

impl ValuePointerContext {
    pub fn new() -> Self {
        ValuePointerContext
    }

    pub fn variable_ident(&self) -> syn::Ident {
        parse_quote!(arg)
    }

    pub(crate) fn compile_value_map_expr<CG: CodeGenerator<Self, StructDef, syn::Expr>>(
        &self,
        code_generator: &CG,
    ) -> syn::Expr {
        let expr = code_generator.generate(self);
        let arg_ident = self.variable_ident();
        parse_quote!(
            {
                let #arg_ident = &record.value;
                let value = #expr;
                arroyo_types::Record {
                timestamp: record.timestamp,
                key: None,
                value
        }
        })
    }

    pub(crate) fn compile_updating_value_map_expression<
        CG: CodeGenerator<Self, StructDef, syn::Expr>,
    >(
        &self,
        code_generator: &CG,
    ) -> syn::Expr {
        let expr = code_generator.generate(self);
        let arg_ident = self.variable_ident();
        parse_quote!(
            {
                let value = record.value.map_over_inner(|#arg_ident| #expr)?;
                Some(arroyo_types::Record {
                timestamp: record.timestamp,
                key: None,
                value
            })
        })
    }

    pub(crate) fn compile_key_map_expression<CG: CodeGenerator<Self, StructDef, syn::Expr>>(
        &self,
        code_generator: &CG,
    ) -> syn::Expr {
        let expr = code_generator.generate(self);
        let arg_ident = self.variable_ident();
        parse_quote!(
                {
                    let #arg_ident = &record.value;
                    let key = #expr;
                    arroyo_types::Record {
                    timestamp: record.timestamp,
                    key: Some(key),
                    value: record.value.clone()
            }
        })
    }

    pub(crate) fn compile_updating_key_map_closure<
        CG: CodeGenerator<Self, StructDef, syn::Expr>,
    >(
        &self,
        code_generator: &CG,
    ) -> syn::ExprClosure {
        let expr = code_generator.generate(self);
        let arg_ident = self.variable_ident();
        parse_quote!(|#arg_ident| {#expr})
    }

    pub(crate) fn compile_filter_expression<CG: CodeGenerator<Self, TypeDef, syn::Expr>>(
        &self,
        code_generator: &CG,
    ) -> syn::Expr {
        let TypeDef::DataType(DataType::Boolean, nullable) = code_generator.expression_type(self)
        else {
            unreachable!("Filter expression must be boolean")
        };
        let expr = code_generator.generate(self);
        let arg_ident = self.variable_ident();
        let unwrap = if nullable {
            Some(quote!(.unwrap_or(false)))
        } else {
            None
        };

        parse_quote!(
            {
                let #arg_ident = &record.value;
                #expr #unwrap
        })
    }

    pub(crate) fn compile_updating_filter_optional_record_expression<
        CG: CodeGenerator<Self, TypeDef, syn::Expr>,
    >(
        &self,
        code_generator: &CG,
    ) -> syn::Expr {
        let TypeDef::DataType(DataType::Boolean, nullable) = code_generator.expression_type(self)
        else {
            unreachable!("Filter expression must be boolean")
        };
        let filter_expr = code_generator.generate(self);
        let arg_ident = self.variable_ident();
        let unwrap = if nullable {
            Some(quote!(.unwrap_or(false)))
        } else {
            None
        };
        parse_quote!(
            {
                let value = record.value.filter(|#arg_ident| #filter_expr #unwrap)?;
                Some(arroyo_types::Record {
                    timestamp: record.timestamp,
                    key: record.key.clone(),
                    value
                })
            }
        )
    }

    pub(crate) fn compile_timestamp_record_expression<
        CG: CodeGenerator<Self, TypeDef, syn::Expr>,
    >(
        &self,
        code_generator: &CG,
    ) -> syn::Expr {
        let TypeDef::DataType(DataType::Timestamp(_, _), nullable) =
            code_generator.expression_type(self)
        else {
            unreachable!("Timestamp expression must be timestamp")
        };
        let timestamp_expr = code_generator.generate(self);
        let arg_ident = self.variable_ident();
        let unwrap_tokens = if nullable {
            Some(quote!(.expect("require a non-null timestamp")))
        } else {
            None
        };
        parse_quote!(
            {
                let #arg_ident = &record.value;
                let timestamp = (#timestamp_expr)#unwrap_tokens;
                arroyo_types::Record {
                    timestamp,
                    key: record.key.clone(),
                    value: record.value.clone()
                }
            }
        )
    }
}
pub struct VecAggregationContext {
    pub values_context: VecOfPointersContext,
}

impl VecAggregationContext {
    pub fn new() -> Self {
        VecAggregationContext {
            values_context: VecOfPointersContext,
        }
    }

    pub fn key_ident(&self) -> syn::Ident {
        parse_quote!(key)
    }

    pub fn window_ident(&self) -> syn::Ident {
        parse_quote!(window)
    }
}

pub struct VecOfPointersContext;

impl VecOfPointersContext {
    pub fn variable_ident(&self) -> syn::Ident {
        parse_quote!(arg)
    }
}

// TWO PHASE CONTEXTS

#[derive(Debug, Clone)]
pub enum BinType {
    Usize,
    DataType(DataType),
    Option(Box<BinType>),
    Tuple(Vec<BinType>),
    BTreeMap(Box<BinType>, Box<BinType>),
}

impl BinType {
    pub fn syn_type(&self) -> syn::Type {
        match self {
            BinType::DataType(data_type) => data_type_as_syn_type(data_type),
            BinType::Option(inner_type) => {
                let inner_syn_type = inner_type.syn_type();
                parse_quote!(Option<#inner_syn_type>)
            }
            BinType::Tuple(inner_types) => match inner_types.len() {
                0 => parse_quote!(()),
                1 => {
                    let inner_syn_type = inner_types.first().unwrap().syn_type();
                    parse_quote!((#inner_syn_type,))
                }
                _ => {
                    let inner_types: Vec<_> = inner_types.iter().map(|t| t.syn_type()).collect();
                    parse_quote!((#(#inner_types),*))
                }
            },
            BinType::BTreeMap(key, value) => {
                let key_ident = key.syn_type();
                let value_ident = value.syn_type();
                parse_quote!(std::collections::BTreeMap<#key_ident, #value_ident>)
            }
            BinType::Usize => parse_quote!(usize),
        }
    }
}

pub struct ValueBinMergingContext {
    pub value_context: ValuePointerContext,
    pub bin_context: BinContext,
}

impl ValueBinMergingContext {
    pub fn new() -> Self {
        ValueBinMergingContext {
            value_context: ValuePointerContext::new(),
            bin_context: BinContext,
        }
    }

    pub fn compile_closure_with_some_result<CG: CodeGenerator<Self, BinType, syn::Expr>>(
        &self,
        code_generator: &CG,
    ) -> syn::ExprClosure {
        let expr = code_generator.generate(self);
        let value_ident = self.value_context.variable_ident();
        let bin_ident = self.bin_context.current_bin_ident();
        parse_quote!(|#value_ident, #bin_ident| { Some(#expr) })
    }

    pub fn compile_closure<CG: CodeGenerator<Self, BinType, syn::Expr>>(
        &self,
        code_generator: &CG,
    ) -> syn::ExprClosure {
        let expr = code_generator.generate(self);
        let value_ident = self.value_context.variable_ident();
        let bin_ident = self.bin_context.current_bin_ident();
        parse_quote!(|#value_ident, #bin_ident| { #expr })
    }

    pub fn bin_syn_type<CG: CodeGenerator<Self, BinType, syn::Expr>>(
        &self,
        code_generator: &CG,
    ) -> syn::Type {
        code_generator.expression_type(self).syn_type()
    }
}

pub struct BinContext;

impl BinContext {
    pub fn current_bin_ident(&self) -> syn::Ident {
        parse_quote!(current_bin)
    }
}

pub struct CombiningContext {
    pub value_context: ValuePointerContext,
    pub bin_context: BinContext,
}

impl CombiningContext {
    pub fn new() -> Self {
        CombiningContext {
            value_context: ValuePointerContext::new(),
            bin_context: BinContext,
        }
    }

    pub fn current_bin_ident(&self) -> syn::Ident {
        parse_quote!(current_bin)
    }
    pub fn new_bin_ident(&self) -> syn::Ident {
        parse_quote!(new_bin)
    }

    pub(crate) fn compile_closure<CG: CodeGenerator<Self, BinType, syn::Expr>>(
        &self,
        code_generator: &CG,
    ) -> syn::ExprClosure {
        let expr = code_generator.generate(self);
        let current_bin_ident = self.current_bin_ident();
        let new_bin_ident = self.new_bin_ident();
        parse_quote!(|#new_bin_ident, #current_bin_ident| { #expr })
    }
}

pub struct MemoryAddingContext {
    pub value_context: ValuePointerContext,
}

impl MemoryAddingContext {
    pub fn new() -> Self {
        MemoryAddingContext {
            value_context: ValuePointerContext::new(),
        }
    }

    pub fn bin_value_ident(&self) -> syn::Ident {
        parse_quote!(bin_value)
    }
    pub fn memory_value_ident(&self) -> syn::Ident {
        parse_quote!(current)
    }

    pub(crate) fn compile_closure<CG: CodeGenerator<Self, BinType, syn::Expr>>(
        &self,
        code_generator: &CG,
    ) -> syn::ExprClosure {
        let in_memory_add = code_generator.generate(self);
        let memory_value_ident = self.memory_value_ident();
        let bin_value_ident = self.bin_value_ident();
        parse_quote!(|#memory_value_ident, #bin_value_ident| {#in_memory_add})
    }

    pub(crate) fn memory_type<CG: CodeGenerator<Self, BinType, syn::Expr>>(
        &self,
        code_generator: &CG,
    ) -> syn::Type {
        code_generator.expression_type(self).syn_type()
    }
}

pub struct MemoryRemovingContext {
    pub value_context: ValuePointerContext,
    pub state_bin_context: BinContext,
    pub memory_bin_context: BinContext,
}

impl MemoryRemovingContext {
    pub fn bin_value_ident(&self) -> syn::Ident {
        parse_quote!(bin_value)
    }
    pub fn memory_value_ident(&self) -> syn::Ident {
        parse_quote!(current)
    }

    pub(crate) fn new() -> Self {
        MemoryRemovingContext {
            value_context: ValuePointerContext::new(),
            state_bin_context: BinContext,
            memory_bin_context: BinContext,
        }
    }

    pub(crate) fn compile_closure<CG: CodeGenerator<Self, BinType, syn::Expr>>(
        &self,
        code_generator: &CG,
    ) -> syn::ExprClosure {
        let in_memory_remove = code_generator.generate(self);
        let memory_value_ident = self.memory_value_ident();
        let bin_value_ident = self.bin_value_ident();
        parse_quote!(|#memory_value_ident, #bin_value_ident| {#in_memory_remove})
    }
}

pub struct BinAggregatingContext {
    pub value_context: ValuePointerContext,
}
impl BinAggregatingContext {
    pub(crate) fn bin_name(&self) -> syn::Ident {
        parse_quote!(bin)
    }

    pub fn key_ident(&self) -> syn::Ident {
        parse_quote!(key)
    }

    pub fn window_ident(&self) -> syn::Ident {
        parse_quote!(window)
    }

    pub(crate) fn new() -> Self {
        Self {
            value_context: ValuePointerContext::new(),
        }
    }

    pub(crate) fn compile_closure<CG: CodeGenerator<Self, StructDef, syn::Expr>>(
        &self,
        code_generator: &CG,
    ) -> syn::ExprClosure {
        let expr = code_generator.generate(self);
        let window_ident = self.window_ident();
        let key_ident = self.key_ident();
        let bin_ident = self.bin_name();
        parse_quote!(|#key_ident, #window_ident, #bin_ident| { #expr })
    }

    pub(crate) fn compile_non_windowed_closure<CG: CodeGenerator<Self, StructDef, syn::Expr>>(
        &self,
        code_generator: &CG,
    ) -> syn::ExprClosure {
        let expr = code_generator.generate(self);
        let key_ident = self.key_ident();
        let bin_ident = self.bin_name();
        parse_quote!(|#key_ident, #bin_ident| { #expr })
    }
}

pub struct MemoryAggregatingContext {
    pub value_context: ValuePointerContext,
    pub mem_context: BinContext,
}
impl MemoryAggregatingContext {
    pub(crate) fn new() -> Self {
        Self {
            value_context: ValuePointerContext::new(),
            mem_context: BinContext,
        }
    }

    pub(crate) fn bin_name(&self) -> syn::Ident {
        parse_quote!(mem_bin)
    }

    pub fn key_ident(&self) -> syn::Ident {
        parse_quote!(key)
    }

    pub fn window_ident(&self) -> syn::Ident {
        parse_quote!(window)
    }

    pub(crate) fn compile_closure<CG: CodeGenerator<Self, StructDef, syn::Expr>>(
        &self,
        code_generator: &CG,
    ) -> syn::ExprClosure {
        let expr = code_generator.generate(self);
        let window_ident = self.window_ident();
        let key_ident = self.key_ident();
        let mem_ident = self.bin_name();
        parse_quote!(|#key_ident, #window_ident, #mem_ident| { #expr })
    }
    pub(crate) fn compile_non_windowed_closure<CG: CodeGenerator<Self, StructDef, syn::Expr>>(
        &self,
        code_generator: &CG,
    ) -> syn::ExprClosure {
        let expr = code_generator.generate(self);
        let key_ident = self.key_ident();
        let mem_ident = self.bin_name();
        parse_quote!(|#key_ident, #mem_ident| { #expr })
    }
}
