extern crate clap;
extern crate consistent_hash;

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::fs::File;
use std::iter::FromIterator;
use std::time::Instant;
use clap::{App, Arg};
use consistent_hash::{StaticHashRing, Node, DefaultHash};

fn main() {
    let matches = App::new("bench")
        .arg(Arg::with_name("WORD_FILE")
            .index(1)
            .required(true))
        .arg(Arg::with_name("NODES")
            .long("nodes")
            .required(true)
            .takes_value(true)
            .min_values(1)
            .multiple(true))
        .arg(Arg::with_name("VNODE_COUNT")
            .long("vnode_count")
            .takes_value(true)
            .default_value("1000"))
        .get_matches();

    let filepath = matches.value_of("WORD_FILE").unwrap();
    let words: Vec<_> = BufReader::new(File::open(filepath).expect("Cannot open file"))
        .lines()
        .collect::<Result<_, _>>()
        .expect("Cannot read words");
    println!("WORD COUNT: {}", words.len());

    let vnodes = matches.value_of("VNODE_COUNT").unwrap().parse().expect("Wrong integer");
    let ring = StaticHashRing::new(DefaultHash,
                                   matches.values_of("NODES")
                                       .unwrap()
                                       .map(|n| Node::new(n).quantity(vnodes)));
    println!("REAL NODE COUNT: {}", ring.nodes().len());
    println!("VIRTUAL NODE COUNT: {} ({} per node)", ring.len(), vnodes);

    let start_time = Instant::now();
    for word in words.iter() {
        ring.calc_candidates(word).nth(0).unwrap();
    }
    let end_time = Instant::now();

    let mut counts: HashMap<&str, _> = HashMap::from_iter(ring.nodes().iter().map(|k| (k.key, 0)));
    for word in words.iter() {
        let selected = ring.calc_candidates(word).nth(0).unwrap();
        *counts.get_mut(selected.key).unwrap() += 1;
    }

    println!("");
    println!("SELECTED COUNT PER NODE:");
    for (node, count) in counts {
        println!("- {}: \t{}", node, count);
    }
    println!("");

    let elapsed = end_time - start_time;
    let elapsed_micros = elapsed.as_secs() * 1_000_000 + (elapsed.subsec_nanos() / 1000) as u64;
    println!("ELAPSED: {} ms", elapsed_micros / 1000);
    println!("WORDS PER SECOND: {}",
             (((words.len() as f64) / (elapsed_micros as f64)) * 1_000_000.0) as u64);
}
