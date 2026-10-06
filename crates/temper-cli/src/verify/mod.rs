//! Verification cascade command for `temper verify`.
//!
//! Validates application files and requires a complete verification result.

use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use temper_spec::automaton::{LintSeverity, lint_automata_bundle, lint_automaton};
use temper_spec::csdl::parse_csdl;
use temper_spec::model::build_spec_model;

mod input;
mod package;
use input::read_ioa_sources;

/// Run the `temper verify` command.
///
/// Loads CSDL and IOA, validates policies and compiled module references, and
/// runs the verification cascade. Missing files and incomplete proofs fail.
pub fn run(specs_dir: &str) -> Result<()> {
    let specs_path = Path::new(specs_dir);
    println!("Running verification cascade...");
    println!("  Specs directory: {}", specs_path.display());

    // Read the CSDL model file
    let csdl_path = specs_path.join("model.csdl.xml");
    if !csdl_path.exists() {
        anyhow::bail!(
            "CSDL model file not found at {}. Run `temper init` first.",
            csdl_path.display()
        );
    }

    let csdl_xml = fs::read_to_string(&csdl_path)
        .with_context(|| format!("Failed to read {}", csdl_path.display()))?;
    input::validate_xml_document(&csdl_xml)?;
    let csdl = parse_csdl(&csdl_xml)
        .with_context(|| format!("Failed to parse CSDL from {}", csdl_path.display()))?;

    anyhow::ensure!(!csdl.schemas.is_empty(), "CSDL must contain a Schema");

    // Read IOA TOML specs (preferred) and TLA+ specs (legacy)
    let ioa_sources = read_ioa_sources(specs_path)?;
    let tla_sources = read_tla_sources(specs_path)?;
    input::validate_ioa_entities(&csdl, &ioa_sources)?;
    package::validate(specs_path)?;

    verify_ioa_sources(&ioa_sources)?;

    // Build spec model (which includes cross-validation)
    let spec = build_spec_model(csdl, tla_sources);

    // Report results
    println!("\nVerification Report");
    println!("{}", "=".repeat(50));

    // Schema summary
    let entity_count: usize = spec.csdl.schemas.iter().map(|s| s.entity_types.len()).sum();
    let action_count: usize = spec.csdl.schemas.iter().map(|s| s.actions.len()).sum();
    let function_count: usize = spec.csdl.schemas.iter().map(|s| s.functions.len()).sum();

    println!("\nSpecification Summary:");
    println!("  Entity types:    {entity_count}");
    println!("  Actions:         {action_count}");
    println!("  Functions:       {function_count}");
    println!("  State machines:  {}", spec.state_machines.len());

    // State machine details
    for (name, sm) in &spec.state_machines {
        println!("\n  State Machine: {name}");
        println!("    States:       {}", sm.states.len());
        println!("    Transitions:  {}", sm.transitions.len());
        println!("    Invariants:   {}", sm.invariants.len());
        println!("    Liveness:     {}", sm.liveness_properties.len());
    }

    // Validation errors
    if !spec.validation.errors.is_empty() {
        println!("\nErrors ({}):", spec.validation.errors.len());
        for err in &spec.validation.errors {
            println!("  FAIL: {err}");
        }
    }

    // Validation warnings
    if !spec.validation.warnings.is_empty() {
        println!("\nWarnings ({}):", spec.validation.warnings.len());
        for warn in &spec.validation.warnings {
            println!("  WARN: {warn}");
        }
    }

    // Summary
    println!("\n{}", "=".repeat(50));
    if spec.validation.is_valid() {
        println!("Result: PASS -- all cross-validation checks passed.");
    } else {
        println!(
            "Result: FAIL -- {} error(s) found.",
            spec.validation.errors.len()
        );
        anyhow::bail!("Verification failed.");
    }

    Ok(())
}

/// Verify IOA behavior independently of artifact packaging. Unit tests also use
/// this to check source collections whose modules and policies are supplied later.
fn verify_ioa_sources(ioa_sources: &std::collections::BTreeMap<String, String>) -> Result<()> {
    // Run IOA verification cascade if IOA files found
    if !ioa_sources.is_empty() {
        let mut parsed_automata = std::collections::BTreeMap::new();
        let mut lint_error_count = 0usize;
        let mut lint_error_lines = Vec::new();

        for (entity_name, ioa_source) in ioa_sources {
            let automaton = temper_spec::automaton::parse_automaton(ioa_source)
                .with_context(|| format!("Failed to parse IOA spec for '{entity_name}'"))?;

            for finding in lint_automaton(&automaton) {
                match finding.severity {
                    LintSeverity::Error => {
                        lint_error_count += 1;
                        lint_error_lines.push(format!(
                            "{entity_name}: {} — {}",
                            finding.code, finding.message
                        ));
                        println!(
                            "\n  [lint:error] {entity_name}: {} — {}",
                            finding.code, finding.message
                        );
                    }
                    LintSeverity::Warning => {
                        println!(
                            "\n  [lint:warn] {entity_name}: {} — {}",
                            finding.code, finding.message
                        );
                    }
                }
            }

            parsed_automata.insert(entity_name.clone(), automaton);
        }

        for finding in lint_automata_bundle(&parsed_automata) {
            match finding.severity {
                LintSeverity::Error => {
                    lint_error_count += 1;
                    lint_error_lines.push(format!(
                        "{}: {} — {}",
                        finding.entity, finding.code, finding.message
                    ));
                    println!(
                        "\n  [lint:error] {}: {} — {}",
                        finding.entity, finding.code, finding.message
                    );
                }
                LintSeverity::Warning => {
                    println!(
                        "\n  [lint:warn] {}: {} — {}",
                        finding.entity, finding.code, finding.message
                    );
                }
            }
        }

        if lint_error_count > 0 {
            anyhow::bail!(
                "IOA lint failed with {lint_error_count} error(s): {}",
                lint_error_lines.join(" | ")
            );
        }

        println!("\nRunning IOA verification cascade...");
        for (entity_name, ioa_source) in ioa_sources {
            println!("\n  Verifying {entity_name}...");
            let cascade = temper_verify::cascade::VerificationCascade::from_ioa(ioa_source)
                .with_sim_seeds(5)
                .with_prop_test_cases(100);
            let result = cascade.run();
            for level in &result.levels {
                let status = if level.passed { "PASS" } else { "FAIL" };
                println!("    [{status}] {}", level.summary);
                // Detail lines localise the failure. Without them a failed
                // level reports only a count, and bisecting the spec is the
                // only way to find out which property broke.
                for line in &level.diagnostics {
                    println!("           {line}");
                }
            }
            if !result.all_passed {
                anyhow::bail!("IOA verification failed for entity '{entity_name}'");
            }
        }
        println!("\nIOA verification cascade: ALL PASSED");

        // ADR-0150: directory verification ALWAYS runs composite cross-entity
        // verification as a first-class, gating step. It composes every
        // entity's joint state machine and BFS-checks that no cross-entity
        // reaction is dropped (target not in its required from-state). This is
        // only meaningful with two or more entities — a single spec has nothing
        // to compose (and stdin verification stays per-entity by design).
        if parsed_automata.len() >= 2 {
            run_composite_verification(&parsed_automata)?;
        }
    }

    Ok(())
}

/// Run always-on composite cross-entity verification over the parsed
/// automata (ADR-0150).
///
/// Seeds the joint-state BFS from the root of each weakly-connected component
/// of the entity trigger graph (so every entity is covered), checks the
/// `no_dropped_reaction` property, and reports every dropped reaction with
/// enough detail to name it. A dropped reaction GATES — it fails the command.
/// An INCOMPLETE run (budget exhausted) fails complete-application verification.
fn run_composite_verification(
    parsed_automata: &std::collections::BTreeMap<String, temper_spec::automaton::Automaton>,
) -> Result<()> {
    use temper_verify::composite::verify_all;

    let automaton_refs: Vec<&temper_spec::automaton::Automaton> =
        parsed_automata.values().collect();

    println!("\nRunning composite cross-entity verification (ADR-0150)...");
    let results = verify_all(&automaton_refs);
    report_composite_results(&results)
}

fn report_composite_results(
    results: &[temper_verify::composite::CompositeVerifyResult],
) -> Result<()> {
    use temper_verify::composite::CompositeOutcome;

    let mut any_violation = false;
    let mut any_incomplete = false;
    let mut dropped_lines: Vec<String> = Vec::new();

    for result in results {
        let scope = result.scope.join(", ");
        match result.outcome {
            CompositeOutcome::Verified => {
                println!(
                    "    [PASS] seed={} scope=[{}] — {} joint states, no dropped reactions",
                    result.seed, scope, result.states_explored,
                );
            }
            CompositeOutcome::Violated => {
                any_violation = true;
                println!(
                    "    [FAIL] seed={} scope=[{}] — {} joint states, {} dropped reaction(s)",
                    result.seed,
                    scope,
                    result.states_explored,
                    result.dropped_reactions.len(),
                );
                for drop in &result.dropped_reactions {
                    let line = format!(
                        "{}.{} fired trigger '{}' targeting {}.{}, but {} was in '{}' (action not enabled) — reaction DROPPED",
                        drop.source_entity,
                        drop.source_action,
                        drop.trigger_name,
                        drop.target_entity,
                        drop.target_action,
                        drop.target_entity,
                        drop.target_state,
                    );
                    println!("           - {line}");
                    dropped_lines.push(line);
                }
                for other in &result.other_violations {
                    println!("           - other violated property: {other}");
                }
            }
            CompositeOutcome::Incomplete => {
                any_incomplete = true;
                println!(
                    "    [INCOMPLETE] seed={} scope=[{}] — explored {} joint states; BFS budget exhausted, proof is PARTIAL (not a pass)",
                    result.seed, scope, result.states_explored,
                );
            }
        }
    }

    if any_violation {
        anyhow::bail!(
            "composite cross-entity verification failed: {} dropped reaction(s):\n  - {}",
            dropped_lines.len(),
            dropped_lines.join("\n  - "),
        );
    }
    if any_incomplete {
        anyhow::bail!("composite verification incomplete: state exploration budget exhausted");
    }
    println!("\nComposite cross-entity verification: ALL PASSED");

    Ok(())
}

// read_tla_sources is shared via crate::util::read_tla_sources
use crate::util::read_tla_sources;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod migration_tests;

#[cfg(test)]
mod repository_tests;
