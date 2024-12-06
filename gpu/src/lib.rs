use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

#[proc_macro_attribute]
pub fn main(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    let block = input.block;

    let expanded = quote! {
        fn main() {
            // Set GPU_MODE_ENABLED to true
            GPU_MODE_ENABLED.store(true, std::sync::atomic::Ordering::SeqCst);
            #block
        }
    };

    expanded.into()
}
