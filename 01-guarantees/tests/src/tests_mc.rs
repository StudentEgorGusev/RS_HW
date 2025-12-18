use std::collections::HashMap;
use std::time::Duration;

use sugars::boxed;

use anysystem::logger::LogEntry;
use anysystem::mc::{
    predicates::{goals, invariants, prunes},
    strategies::Bfs,
    InvariantFn, ModelChecker, StrategyConfig,
};
use anysystem::test::TestResult;
use anysystem::Message;

use crate::common::{
    build_system, check_delivered_messages, check_message_delivery_once, check_message_delivery_ordered,
    check_message_delivery_reliable, generate_message_texts, TestConfig,
};

fn mc_invariant_guarantees(messages_expected: Vec<Message>, config: TestConfig) -> InvariantFn {
    boxed!(move |state| {
        let mut expected_msg_count = HashMap::new();
        for msg in &messages_expected {
            *expected_msg_count.entry(msg.data.clone()).or_insert(0) += 1;
        }
        let delivered = &state.node_states["receiver-node"].proc_states["receiver"].local_outbox;

        // check that delivered messages have expected type and data
        let delivered_msg_count = check_delivered_messages(delivered, &expected_msg_count, &messages_expected[0].tip)?;

        // check delivered message count according to expected guarantees
        if config.reliable && state.events.is_empty() {
            check_message_delivery_reliable(&delivered_msg_count, &expected_msg_count)?;
        }
        if config.once {
            check_message_delivery_once(&delivered_msg_count, &expected_msg_count)?;
        }
        if config.ordered {
            check_message_delivery_ordered(delivered, &messages_expected)?;
        }
        Ok(())
    })
}

pub fn test_mc_reliable_network(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let messages: Vec<Message> = generate_message_texts(&mut sys, 2)
        .into_iter()
        .map(|text| Message::new("MESSAGE", &format!(r#"{{"text": "{text}"}}"#)))
        .collect();
    let strategy_config = StrategyConfig::default()
        .prune(prunes::sent_messages_limit(4))
        .goal(goals::got_n_local_messages("receiver-node", "receiver", 2))
        .invariant(invariants::all_invariants(vec![
            invariants::state_depth(20),
            mc_invariant_guarantees(messages.clone(), *config),
        ]));
    let mut mc = ModelChecker::new(&sys);
    let res = mc.run_with_change::<Bfs>(strategy_config, move |sys| {
        for message in messages {
            sys.send_local_message("sender-node", "sender", message);
        }
    });
    if let Err(e) = res {
        e.print_trace();
        Err(e.message())
    } else {
        Ok(true)
    }
}

pub fn test_mc_message_drops(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    sys.network().set_drop_rate(0.1);
    let messages: Vec<Message> = generate_message_texts(&mut sys, 2)
        .into_iter()
        .map(|text| Message::new("MESSAGE", &format!(r#"{{"text": "{text}"}}"#)))
        .collect();
    let strategy_config = StrategyConfig::default()
        .prune(prunes::state_depth(7))
        .goal(goals::any_goal(vec![
            goals::got_n_local_messages("receiver-node", "receiver", 2),
            goals::no_events(),
        ]))
        .invariant(mc_invariant_guarantees(messages.clone(), *config));
    let mut mc = ModelChecker::new(&sys);
    let res = mc.run_with_change::<Bfs>(strategy_config, move |sys| {
        for message in messages {
            sys.send_local_message("sender-node", "sender", message);
        }
    });
    if let Err(e) = res {
        e.print_trace();
        Err(e.message())
    } else {
        Ok(true)
    }
}

pub fn test_mc_unstable_network(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    sys.network().set_drop_rate(0.1);
    sys.network().set_dupl_rate(0.1);
    let msg_count = if config.ordered { 3 } else { 2 };
    let messages: Vec<Message> = generate_message_texts(&mut sys, msg_count)
        .into_iter()
        .map(|text| Message::new("MESSAGE", &format!(r#"{{"text": "{text}"}}"#)))
        .collect();
    let num_drops_allowed = 1;
    let num_duplication_allowed = 1;
    let goal = if config.reliable && config.once {
        goals::all_goals(vec![
            goals::got_n_local_messages("receiver-node", "receiver", msg_count),
            goals::no_events(),
        ])
    } else {
        goals::no_events()
    };
    let mut invariants = vec![
        invariants::state_depth(20),
        mc_invariant_guarantees(messages.clone(), *config),
    ];
    if config.ordered {
        invariants.push(invariants::time_limit(Duration::from_secs(80)))
    };
    let strategy_config = StrategyConfig::default()
        .prune(prunes::any_prune(vec![
            prunes::events_limit(LogEntry::is_mc_message_dropped, num_drops_allowed),
            prunes::events_limit(LogEntry::is_mc_message_duplicated, num_duplication_allowed),
            prunes::events_limit(LogEntry::is_mc_timer_fired, 1),
            prunes::events_limit(LogEntry::is_mc_message_received, msg_count + num_drops_allowed),
        ]))
        .goal(goal)
        .invariant(invariants::all_invariants(invariants));
    let mut mc = ModelChecker::new(&sys);

    let res = mc.run_with_change::<Bfs>(strategy_config, |sys| {
        for msg in messages {
            sys.send_local_message("sender-node", "sender", msg.clone());
        }
    });
    if let Err(e) = res {
        e.print_trace();
        Err(e.message())
    } else {
        Ok(true)
    }
}
