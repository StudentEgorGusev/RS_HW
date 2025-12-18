mod common;
mod tests;
mod tests_mc;

use std::collections::{BTreeMap, HashSet};
use std::env;
use std::io::Write;

use clap::Parser;
use env_logger::Builder;
use log::LevelFilter;

use anysystem::test::{TestResult, TestSuite};

use crate::common::TestConfig;
use crate::tests::*;
use crate::tests_mc::*;

/// Guarantees Homework Tests
#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
struct Args {
    /// Path to Python file with solution
    #[clap(long = "impl", short = 'i', default_value = "solution/guarantees.py")]
    solution_path: String,

    /// Test to run (optional)
    #[clap(long = "test", short)]
    test: Option<String>,

    /// Print execution trace
    #[clap(long, short)]
    debug: bool,

    /// Guarantee to check
    #[clap(long, short, possible_values = ["AMO", "ALO", "EO", "EOO"])]
    guarantee: Option<String>,

    /// Random seed used in tests
    #[clap(long, short, default_value = "123")]
    seed: u64,

    /// Number of chaos monkey runs
    #[clap(long, short, default_value = "0")]
    monkeys: u32,

    /// Run overhead tests
    #[clap(long, short)]
    overhead: bool,

    /// Run model checking tests
    #[clap(long, short = 'c')]
    model_checking: bool,
}

fn main() {
    let args = Args::parse();
    if args.debug {
        Builder::new()
            .filter(Some("anysystem"), LevelFilter::Debug)
            .format(|buf, record| writeln!(buf, "{}", record.args()))
            .init();
    }
    let guarantee = args.guarantee.as_deref();

    append_to_python_path("../../anysystem/python".to_string());
    env::set_var("PYTHONHASHSEED", args.seed.to_string());
    let mut config = TestConfig {
        impl_path: &args.solution_path,
        sender_class: "",
        receiver_class: "",
        seed: args.seed,
        monkeys: args.monkeys,
        reliable: false,
        once: false,
        ordered: false,
    };
    let mut tests = TestSuite::new();

    // At most once
    if guarantee.is_none() || guarantee == Some("AMO") {
        config.sender_class = "AtMostOnceSender";
        config.receiver_class = "AtMostOnceReceiver";
        config.once = true;
        // without drops should be reliable
        config.reliable = true;
        tests.add("[AT MOST ONCE] NORMAL", test_normal, config);
        tests.add("[AT MOST ONCE] NORMAL NON-UNIQUE", test_normal_non_unique, config);
        tests.add("[AT MOST ONCE] DELAYED", test_delayed, config);
        tests.add("[AT MOST ONCE] DUPLICATED", test_duplicated, config);
        tests.add("[AT MOST ONCE] DELAYED+DUPLICATED", test_delayed_duplicated, config);
        // with drops is not reliable
        config.reliable = false;
        tests.add("[AT MOST ONCE] DROPPED", test_dropped, config);
        if args.monkeys > 0 {
            tests.add("[AT MOST ONCE] CHAOS MONKEY", test_chaos_monkey, config);
        }
        if args.overhead {
            config.reliable = true;
            tests.add(
                "[AT MOST ONCE] OVERHEAD NORMAL",
                |x| test_overhead(x, "AMO", false),
                config,
            );
            config.reliable = false;
            tests.add(
                "[AT MOST ONCE] OVERHEAD FAULTY",
                |x| test_overhead(x, "AMO", true),
                config,
            );
        }
        if args.model_checking {
            tests.add("[AT MOST ONCE] MODEL CHECKING", test_mc_reliable_network, config);
            tests.add(
                "[AT MOST ONCE] MODEL CHECKING MESSAGE DROPS",
                test_mc_message_drops,
                config,
            );
            tests.add(
                "[AT MOST ONCE] MODEL CHECKING UNSTABLE NETWORK",
                test_mc_unstable_network,
                config,
            );
        }
    }

    // At least once
    if guarantee.is_none() || guarantee == Some("ALO") {
        config.sender_class = "AtLeastOnceSender";
        config.receiver_class = "AtLeastOnceReceiver";
        config.reliable = true;
        config.once = false;
        tests.add("[AT LEAST ONCE] NORMAL", test_normal, config);
        tests.add("[AT LEAST ONCE] NORMAL NON-UNIQUE", test_normal_non_unique, config);
        tests.add("[AT LEAST ONCE] DELAYED", test_delayed, config);
        tests.add("[AT LEAST ONCE] DUPLICATED", test_duplicated, config);
        tests.add("[AT LEAST ONCE] DELAYED+DUPLICATED", test_delayed_duplicated, config);
        tests.add("[AT LEAST ONCE] DROPPED", test_dropped, config);
        if args.monkeys > 0 {
            tests.add("[AT LEAST ONCE] CHAOS MONKEY", test_chaos_monkey, config);
        }
        if args.overhead {
            tests.add(
                "[AT LEAST ONCE] OVERHEAD NORMAL",
                |x| test_overhead(x, "ALO", false),
                config,
            );
            tests.add(
                "[AT LEAST ONCE] OVERHEAD FAULTY",
                |x| test_overhead(x, "ALO", true),
                config,
            );
        }
        if args.model_checking {
            tests.add("[AT LEAST ONCE] MODEL CHECKING", test_mc_reliable_network, config);
            tests.add(
                "[AT LEAST ONCE] MODEL CHECKING MESSAGE DROPS",
                test_mc_message_drops,
                config,
            );
            tests.add(
                "[AT LEAST ONCE] MODEL CHECKING UNSTABLE NETWORK",
                test_mc_unstable_network,
                config,
            );
        }
    }

    // Exactly once
    if guarantee.is_none() || guarantee == Some("EO") {
        config.sender_class = "ExactlyOnceSender";
        config.receiver_class = "ExactlyOnceReceiver";
        config.reliable = true;
        config.once = true;
        tests.add("[EXACTLY ONCE] NORMAL", test_normal, config);
        tests.add("[EXACTLY ONCE] NORMAL NON-UNIQUE", test_normal_non_unique, config);
        tests.add("[EXACTLY ONCE] DELAYED", test_delayed, config);
        tests.add("[EXACTLY ONCE] DUPLICATED", test_duplicated, config);
        tests.add("[EXACTLY ONCE] DELAYED+DUPLICATED", test_delayed_duplicated, config);
        tests.add("[EXACTLY ONCE] DROPPED", test_dropped, config);
        if args.monkeys > 0 {
            tests.add("[EXACTLY ONCE] CHAOS MONKEY", test_chaos_monkey, config);
        }
        if args.overhead {
            tests.add(
                "[EXACTLY ONCE] OVERHEAD NORMAL",
                |x| test_overhead(x, "EO", false),
                config,
            );
            tests.add(
                "[EXACTLY ONCE] OVERHEAD FAULTY",
                |x| test_overhead(x, "EO", true),
                config,
            );
        }
        if args.model_checking {
            tests.add("[EXACTLY ONCE] MODEL CHECKING", test_mc_reliable_network, config);
            tests.add(
                "[EXACTLY ONCE] MODEL CHECKING MESSAGE DROPS",
                test_mc_message_drops,
                config,
            );
            tests.add(
                "[EXACTLY ONCE] MODEL CHECKING UNSTABLE NETWORK",
                test_mc_unstable_network,
                config,
            );
        }
    }

    // EXACTLY ONCE ORDERED
    if guarantee.is_none() || guarantee == Some("EOO") {
        config.sender_class = "ExactlyOnceOrderedSender";
        config.receiver_class = "ExactlyOnceOrderedReceiver";
        config.reliable = true;
        config.once = true;
        config.ordered = true;
        tests.add("[EXACTLY ONCE ORDERED] NORMAL", test_normal, config);
        tests.add(
            "[EXACTLY ONCE ORDERED] NORMAL NON-UNIQUE",
            test_normal_non_unique,
            config,
        );
        tests.add("[EXACTLY ONCE ORDERED] DELAYED", test_delayed, config);
        tests.add("[EXACTLY ONCE ORDERED] DUPLICATED", test_duplicated, config);
        tests.add(
            "[EXACTLY ONCE ORDERED] DELAYED+DUPLICATED",
            test_delayed_duplicated,
            config,
        );
        tests.add("[EXACTLY ONCE ORDERED] DROPPED", test_dropped, config);
        if args.monkeys > 0 {
            tests.add("[EXACTLY ONCE ORDERED] CHAOS MONKEY", test_chaos_monkey, config);
        }
        if args.overhead {
            tests.add(
                "[EXACTLY ONCE ORDERED] OVERHEAD NORMAL",
                |x| test_overhead(x, "EOO", false),
                config,
            );
            tests.add(
                "[EXACTLY ONCE ORDERED] OVERHEAD FAULTY",
                |x| test_overhead(x, "EOO", true),
                config,
            );
        }
        if args.model_checking {
            tests.add(
                "[EXACTLY ONCE ORDERED] MODEL CHECKING",
                test_mc_reliable_network,
                config,
            );
            tests.add(
                "[EXACTLY ONCE ORDERED] MODEL CHECKING MESSAGE DROPS",
                test_mc_message_drops,
                config,
            );
            tests.add(
                "[EXACTLY ONCE ORDERED] MODEL CHECKING UNSTABLE NETWORK",
                test_mc_unstable_network,
                config,
            );
        }
    }

    if args.test.is_none() {
        let (_, results) = tests.run();
        let score = score(results);
        println!("SCORE: {score}\n");
    } else {
        tests.run_test(&args.test.unwrap());
    }
}

fn score(results: BTreeMap<String, TestResult>) -> f32 {
    let guarantees = HashSet::from(["AT MOST ONCE", "AT LEAST ONCE", "EXACTLY ONCE", "EXACTLY ONCE ORDERED"]);
    let mut failed_guarantees: HashSet<&str> = HashSet::new();
    let mut failed_overheads: HashSet<&str> = HashSet::new();
    for (test, result) in results {
        if result.is_err() {
            for guarantee in guarantees.iter() {
                if test.contains(format!("[{guarantee}]").as_str()) {
                    if test.contains("OVERHEAD") {
                        failed_overheads.insert(guarantee);
                    } else {
                        failed_guarantees.insert(guarantee);
                    }
                }
            }
        }
    }
    9. - failed_guarantees.len() as f32 * 2. - f32::from(!failed_overheads.is_empty())
}

fn append_to_python_path(entry: String) {
    let path_separator = if cfg!(windows) { ";" } else { ":" };
    let current_path = env::var("PYTHONPATH").unwrap_or_default();
    let updated_path = if current_path.is_empty() {
        entry
    } else {
        format!("{current_path}{path_separator}{entry}")
    };
    env::set_var("PYTHONPATH", updated_path);
}
