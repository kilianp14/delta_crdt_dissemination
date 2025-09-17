use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemStruct};

#[proc_macro_attribute]
pub fn experiment(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);
    let name = &input.ident;

    let static_name = syn::Ident::new(
        &format!("__EXPERIMENT_INSTANCE_{}", name),
        name.span(),
    );

    let expanded = quote! {
        #input
        
        static #static_name: #name = #name;

        inventory::submit! {
            ExperimentFactory(&#static_name)
        }
    };

    expanded.into()
}