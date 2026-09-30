//! Regenerates the protobuf bindings shipped in `src/proto/generated`.
//!
//! Run from the repository root with `cargo run -p codegen`. No `protoc` install is required:
//! the schema is parsed with `protox` (pure Rust) and fed into `prost-build`.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use heck::ToUpperCamelCase;
use prost_types::field_descriptor_proto::Type;

const COMMON_TYPE: &str = ".webcast.shared.message.CommonMessageData";

/// Messages carrying a `common` field that are nested payloads rather than top-level events
/// (mirrors the exclusions of `WebcastEventMessage` in the Node library).
const EXCLUDED_EVENT_MESSAGES: &[&str] = &["VaultGloveCardInfo", "Top2CardInfo", "Top3CardInfo"];

fn main() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let proto_root = root.join("proto");
    let out_dir = root.join("src").join("proto").join("generated");

    let mut files = Vec::new();
    collect_protos(&proto_root, &mut files)?;
    files.sort();
    let relative: Vec<PathBuf> = files.iter().map(|f| f.strip_prefix(&proto_root).unwrap().to_path_buf()).collect();

    let mut fds = protox::compile(&relative, [&proto_root]).context("failed to parse .proto files")?;
    disambiguate(&mut fds);

    if out_dir.exists() {
        fs::remove_dir_all(&out_dir)?;
    }
    fs::create_dir_all(&out_dir)?;

    let mut config = prost_build::Config::new();
    config
        .out_dir(&out_dir)
        .include_file("mod.rs")
        .bytes(["."])
        .btree_map(["."])
        .message_attribute(
            ".",
            "#[cfg_attr(feature = \"serde\", derive(serde::Serialize, serde::Deserialize))]\n\
             #[cfg_attr(feature = \"serde\", serde(rename_all = \"camelCase\", default))]",
        )
        .enum_attribute(".", "#[cfg_attr(feature = \"serde\", derive(serde::Serialize, serde::Deserialize))]");
    config.compile_fds(fds.clone())?;

    write_registry(&fds, &out_dir.join("registry.rs"))?;
    println!("generated bindings into {}", out_dir.display());
    Ok(())
}

fn collect_protos(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_protos(&path, out)?;
        } else if path.extension().is_some_and(|e| e == "proto") {
            out.push(path);
        }
    }
    Ok(())
}

/// Renames top-level types whose Rust names collide once converted to UpperCamelCase (e.g.
/// `SubPinCardText_TextType` and `SubPinCard_Text_TextType`), updating every reference.
fn disambiguate(fds: &mut prost_types::FileDescriptorSet) {
    let mut seen: BTreeMap<(String, String), usize> = BTreeMap::new();
    let mut renames: BTreeMap<String, String> = BTreeMap::new();

    for file in &mut fds.file {
        let package = file.package().to_owned();
        let names = file
            .message_type
            .iter_mut()
            .map(|m| m.name.as_mut().unwrap())
            .chain(file.enum_type.iter_mut().map(|e| e.name.as_mut().unwrap()));
        for name in names {
            let count = seen.entry((package.clone(), name.to_upper_camel_case())).or_insert(0);
            *count += 1;
            if *count > 1 {
                let renamed = format!("{name}_{count}");
                println!("renaming {package}.{name} to {renamed}");
                renames.insert(format!(".{package}.{name}"), format!(".{package}.{renamed}"));
                *name = renamed;
            }
        }
    }

    fn visit(message: &mut prost_types::DescriptorProto, renames: &BTreeMap<String, String>) {
        for field in &mut message.field {
            if let Some(renamed) = field.type_name.as_ref().and_then(|t| renames.get(t)) {
                field.type_name = Some(renamed.clone());
            }
        }
        for nested in &mut message.nested_type {
            visit(nested, renames);
        }
    }
    for file in &mut fds.file {
        for message in &mut file.message_type {
            visit(message, &renames);
        }
    }
}

/// Mirrors prost's module naming for package segments that collide with Rust keywords.
fn escape_keyword(segment: &str) -> String {
    const KEYWORDS: &[&str] = &[
        "as", "async", "await", "break", "const", "continue", "dyn", "else", "enum", "extern", "fn", "for", "if", "impl", "in",
        "let", "loop", "match", "mod", "move", "mut", "pub", "ref", "return", "static", "struct", "trait", "type", "unsafe",
        "use", "where", "while", "abstract", "become", "box", "do", "final", "gen", "macro", "override", "priv", "try", "typeof",
        "unsized", "virtual", "yield",
    ];
    if KEYWORDS.contains(&segment) { format!("r#{segment}") } else { segment.to_owned() }
}

/// Emits the list of top-level event messages (every message with a `common: CommonMessageData`
/// field) as a single `webcast_messages!` invocation consumed by `src/proto/mod.rs`.
fn write_registry(fds: &prost_types::FileDescriptorSet, path: &Path) -> Result<()> {
    let mut events: BTreeMap<String, String> = BTreeMap::new();

    for file in &fds.file {
        let package = file.package();
        for message in &file.message_type {
            let name = message.name();
            if EXCLUDED_EVENT_MESSAGES.contains(&name) {
                continue;
            }
            let has_common =
                message.field.iter().any(|f| f.name() == "common" && f.r#type() == Type::Message && f.type_name() == COMMON_TYPE);
            if !has_common {
                continue;
            }
            let module: Vec<String> = package.split('.').map(escape_keyword).collect();
            let rust_path = format!("{}::{}", module.join("::"), name.to_upper_camel_case());
            if let Some(previous) = events.insert(name.to_owned(), rust_path.clone()) {
                bail!("duplicate event message name {name}: {previous} and {rust_path}");
            }
        }
    }

    let mut out = String::from("// @generated by `cargo run -p codegen`. Do not edit.\n\nwebcast_messages! {\n");
    for (name, rust_path) in &events {
        writeln!(out, "    {name} => {rust_path},")?;
    }
    out.push_str("}\n");
    fs::write(path, out)?;
    println!("{} event message types", events.len());
    Ok(())
}
