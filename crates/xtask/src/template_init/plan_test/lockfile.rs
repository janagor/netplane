use super::*;

#[test]
fn test_library_profile_keeps_cargo_lock_ignored() {
    let library_profile_toml = r#"
[metadata]
id = "library"
display_name = "Rust Library"
description = "library profile"
[workspace]
include_crates = ["crates/example-crate"]
[ci]
default_tier = "protected-branch"
[policy]
lockfile = "ignored"
publish_packages = []
[post_init]
checklist = ["item-a"]
"#;
    let (_dir, root) = fixture();
    let blueprint = TemplateProfile::from_toml(library_profile_toml).unwrap();
    let plan = InitPlan::build(&root, &blueprint, &identity()).unwrap();

    assert!(!plan.lockfile_committed);
    assert!(plan.gitignore.is_none());

    apply::execute(&plan).unwrap();
    let gitignore = fs::read_to_string(root.join(".gitignore")).unwrap();
    assert!(
        gitignore.contains("Cargo.lock"),
        "library profile must keep Cargo.lock in .gitignore"
    );
}

#[test]
fn test_binary_profile_unignores_cargo_lock() {
    let binary_profile_toml = r#"
[metadata]
id = "cli"
display_name = "Rust CLI"
description = "cli profile"
[workspace]
include_crates = ["crates/example-crate"]
[ci]
default_tier = "protected-branch"
[policy]
lockfile = "committed"
publish_packages = []
[post_init]
checklist = ["item-a"]
"#;
    let (_dir, root) = fixture();
    let blueprint = TemplateProfile::from_toml(binary_profile_toml).unwrap();
    let plan = InitPlan::build(&root, &blueprint, &identity()).unwrap();

    assert!(plan.lockfile_committed);
    assert!(plan.gitignore.is_some());

    apply::execute(&plan).unwrap();
    let gitignore = fs::read_to_string(root.join(".gitignore")).unwrap();
    assert!(
        !gitignore.contains("Cargo.lock"),
        "binary profile must remove Cargo.lock from .gitignore"
    );
}
