//! Equal-weight routing policy regression tests.
use netplane::routing::{UniformRouting, UniformRoutingError};

#[test]
fn equal_weight_selection_supports_non_percentage_counts() -> Result<(), UniformRoutingError> {
    let policy = UniformRouting::try_new((0..3).map(|index| index.to_string()))?;
    let mut counts = [0; 3];
    for index in 0..9000 {
        let key = index.to_string();
        let selected = policy.select(key.as_bytes());
        assert_eq!(selected, policy.select(key.as_bytes()));
        if let Some(count) = counts.get_mut(selected.parse::<usize>().unwrap_or(3)) {
            *count += 1;
        }
    }
    assert!(counts.iter().all(|&count| (2700..3300).contains(&count)));
    assert_eq!(
        UniformRouting::try_new([]).err(),
        Some(UniformRoutingError::EmptyTargets)
    );
    assert_eq!(
        UniformRouting::try_new(["a".into(), "a".into()]).err(),
        Some(UniformRoutingError::InvalidTarget)
    );
    let many = UniformRouting::try_new((0..257).map(|index| index.to_string()))?;
    assert!(!many.select(b"key").is_empty());
    Ok(())
}
