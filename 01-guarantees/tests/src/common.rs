use std::collections::HashMap;

use assertables::{assume, assume_eq};
use sugars::boxed;

use anysystem::python::PyProcessFactory;
use anysystem::test::TestResult;
use anysystem::{Message, System};

#[derive(Copy, Clone)]
pub struct TestConfig<'a> {
    pub impl_path: &'a str,
    pub sender_class: &'a str,
    pub receiver_class: &'a str,
    pub seed: u64,
    pub monkeys: u32,
    pub reliable: bool,
    pub once: bool,
    pub ordered: bool,
}

pub fn build_system(config: &TestConfig, measure_max_size: bool) -> System {
    let mut sys = System::new(config.seed);
    sys.add_node("sender-node");
    sys.add_node("receiver-node");

    let sender_f = PyProcessFactory::new(config.impl_path, config.sender_class);
    let mut sender = sender_f.build(("sender", "receiver"), config.seed);
    if measure_max_size {
        sender.set_max_size_freq(100);
    }
    sys.add_process("sender", boxed!(sender), "sender-node");

    let receiver_f = PyProcessFactory::new(config.impl_path, config.receiver_class);
    let mut receiver = receiver_f.build(("receiver",), config.seed);
    if measure_max_size {
        receiver.set_max_size_freq(100);
    }
    sys.add_process("receiver", boxed!(receiver), "receiver-node");

    sys
}

pub fn generate_message_texts(sys: &mut System, message_count: usize) -> Vec<String> {
    if message_count == 5 {
        ["distributed", "systems", "need", "some", "guarantees"]
            .map(String::from)
            .to_vec()
    } else {
        let mut messages = Vec::new();
        for _i in 0..message_count {
            let msg = if message_count == 10 {
                format!("{}C", sys.gen_range(20..30))
            } else {
                sys.random_string(100)
            };
            messages.push(msg);
        }
        messages
    }
}

pub fn send_messages(sys: &mut System, message_count: usize) -> Vec<Message> {
    let texts = generate_message_texts(sys, message_count);
    let mut messages = Vec::new();
    for text in texts {
        let msg = Message::new("MESSAGE", &format!(r#"{{"text": "{text}"}}"#));
        sys.send_local_message("sender", msg.clone());
        if message_count <= 50 {
            let steps = sys.gen_range(0..2);
            if steps > 0 {
                sys.steps(steps);
            }
        } else {
            let duration = sys.gen_range(0.0..2.0);
            sys.step_for_duration(duration);
        };
        messages.push(msg);
    }
    messages
}

pub fn check_delivered_messages(
    delivered: &[Message],
    expected_msg_count: &HashMap<String, i32>,
    expected_tip: &String,
) -> Result<HashMap<String, i32>, String> {
    assert!(!expected_msg_count.is_empty());
    let mut delivered_msg_count = HashMap::default();
    for msg in delivered.iter() {
        // assuming all messages have the same type
        assume_eq!(msg.tip, *expected_tip, format!("Wrong message type {}", msg.tip))?;
        assume!(
            expected_msg_count.contains_key(&msg.data),
            format!("Wrong message data: {}", msg.data)
        )?;
        *delivered_msg_count.entry(msg.data.clone()).or_insert(0) += 1;
    }
    Ok(delivered_msg_count)
}

pub fn check_message_delivery_reliable(
    delivered_msg_count: &HashMap<String, i32>,
    expected_msg_count: &HashMap<String, i32>,
) -> TestResult {
    for (data, expected_count) in expected_msg_count {
        let delivered_count = delivered_msg_count.get(data).unwrap_or(&0);
        assume!(
            delivered_count >= expected_count,
            format!(
                "Message {} is not delivered (observed count {} < expected count {})",
                data, delivered_count, expected_count
            )
        )?;
    }
    Ok(true)
}

pub fn check_message_delivery_once(
    delivered_msg_count: &HashMap<String, i32>,
    expected_msg_count: &HashMap<String, i32>,
) -> TestResult {
    for (data, delivered_count) in delivered_msg_count {
        if expected_msg_count.contains_key(data) {
            let expected_count = expected_msg_count[data];
            assume!(
                *delivered_count <= expected_count,
                format!(
                    "Message {} is delivered more than once (observed count {} > expected count {})",
                    data, delivered_count, expected_count
                )
            )?;
        }
    }
    Ok(true)
}

pub fn check_message_delivery_ordered(delivered: &[Message], sent: &[Message]) -> TestResult {
    let mut next_idx = 0;
    for i in 0..delivered.len() {
        let msg = &delivered[i];
        let mut matched = false;
        while !matched && next_idx < sent.len() {
            if msg.data == sent[next_idx].data {
                matched = true;
            } else {
                next_idx += 1;
            }
        }
        assume!(
            matched,
            format!("Order violation: {} after {}", msg.data, &delivered[i - 1].data)
        )?;
    }
    Ok(true)
}

pub fn check_guarantees(sys: &mut System, sent: &[Message], config: &TestConfig) -> TestResult {
    let mut expected_msg_count = HashMap::new();
    for msg in sent {
        *expected_msg_count.entry(msg.data.clone()).or_insert(0) += 1;
    }
    let delivered = sys.read_local_messages("receiver");

    // check that delivered messages have expected type and data
    let delivered_msg_count = check_delivered_messages(&delivered, &expected_msg_count, &sent[0].tip)?;

    // check delivered message count according to expected guarantees
    if config.reliable {
        check_message_delivery_reliable(&delivered_msg_count, &expected_msg_count)?;
    }
    if config.once {
        check_message_delivery_once(&delivered_msg_count, &expected_msg_count)?;
    }
    if config.ordered {
        check_message_delivery_ordered(&delivered, sent)?;
    }
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
pub fn check_overhead(
    guarantee: &str,
    faulty: bool,
    message_count: usize,
    sender_mem: u64,
    receiver_mem: u64,
    net_message_count: u64,
    net_traffic: u64,
    throughput: f64,
) -> TestResult {
    let (sender_mem_limit, receiver_mem_limit, net_message_count_limit, net_traffic_limit, throughput_limit) =
        match guarantee {
            "AMO" => match message_count {
                100 => {
                    if !faulty {
                        (800, 1500, 100, 20000, 0.6)
                    } else {
                        (800, 3500, 100, 20000, 0.6)
                    }
                }
                1000 => {
                    if !faulty {
                        (800, 1500, 1000, 200000, 0.6)
                    } else {
                        (800, 30000, 1000, 200000, 0.6)
                    }
                }
                _ => (u64::MAX, u64::MAX, u64::MAX, u64::MAX, 0.),
            },
            "ALO" => match message_count {
                100 => {
                    if !faulty {
                        (2200, 600, 200, 20000, 0.6)
                    } else {
                        (12000, 600, 500, 40000, 0.6)
                    }
                }
                1000 => {
                    if !faulty {
                        (4200, 600, 2000, 200000, 0.6)
                    } else {
                        (15000, 600, 5000, 400000, 0.6)
                    }
                }
                _ => (u64::MAX, u64::MAX, u64::MAX, u64::MAX, 0.),
            },
            "EO" => match message_count {
                100 => {
                    if !faulty {
                        (2200, 1500, 200, 20000, 0.6)
                    } else {
                        (12000, 2200, 500, 40000, 0.6)
                    }
                }
                1000 => {
                    if !faulty {
                        (4200, 1500, 2000, 200000, 0.6)
                    } else {
                        (15000, 2200, 5000, 400000, 0.6)
                    }
                }
                _ => (u64::MAX, u64::MAX, u64::MAX, u64::MAX, 0.),
            },
            "EOO" => match message_count {
                100 => {
                    if !faulty {
                        (3500, 1200, 200, 25000, 0.4)
                    } else {
                        (30000, 6000, 500, 45000, 0.4)
                    }
                }
                1000 => {
                    if !faulty {
                        (6000, 1200, 2000, 250000, 0.4)
                    } else {
                        (200000, 10000, 5000, 450000, 0.4)
                    }
                }
                _ => (u64::MAX, u64::MAX, u64::MAX, u64::MAX, 0.),
            },
            _ => (u64::MAX, u64::MAX, u64::MAX, u64::MAX, 0.),
        };
    assume!(
        sender_mem <= sender_mem_limit,
        format!("Sender memory > {}", sender_mem_limit)
    )?;
    assume!(
        receiver_mem <= receiver_mem_limit,
        format!("Receiver memory > {}", receiver_mem_limit)
    )?;
    assume!(
        net_message_count <= net_message_count_limit,
        format!("Message count > {}", net_message_count_limit)
    )?;
    assume!(
        net_traffic <= net_traffic_limit,
        format!("Traffic > {}", net_traffic_limit)
    )?;
    assume!(
        throughput >= throughput_limit,
        format!("Throughput < {}", throughput_limit)
    )?;
    Ok(true)
}
