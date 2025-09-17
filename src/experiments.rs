use delta_crdt_dissemination::experiment;
use crate::experiment_engine::{Experiment, ExperimentFactory};

#[experiment]
#[derive(Debug)]
struct ExperimentA;

impl Experiment for ExperimentA {
    fn run(&self) {
        println!("Running ExperimentA");
    }
    fn name(&self) -> &'static str {
        "ExperimentA"
    }
}

#[experiment]
#[derive(Debug)]
struct ExperimentB;

impl Experiment for ExperimentB {
    fn run(&self) {
        println!("Running ExperimentB");
    }

    fn name(&self) -> &'static str {
        "ExperimentB"
    }
}

#[experiment]
#[derive(Debug)]
struct ExperimentC;

impl Experiment for ExperimentC {
    fn run(&self) {
        println!("Running ExperimentC");
    }

    fn name(&self) -> &'static str {
        "ExperimentC"
    }
}