use std::collections::HashMap;
use std::fmt::{format, Debug};

pub trait Experiment: Send + Sync + Debug {
    fn run(&self);
    fn name(&self) -> &'static str;
}

pub struct ExperimentFactory(pub &'static dyn Experiment);

inventory::collect!(ExperimentFactory);

pub struct ExperimentEngine {
    experiment: Box<&'static dyn Experiment>
}

impl ExperimentEngine {
    pub fn new(experiment_str: &str) -> Self {
        let mut experiments_map = HashMap::new();
        for reg in inventory::iter::<ExperimentFactory> {
            let exp = reg.0;
            experiments_map.insert(exp.name(), exp);
        }
        let experiment = Box::new(*experiments_map.get(&experiment_str)
            .expect(format!("Invalid experiment: {experiment_str}").as_str()));
        Self { experiment }
    }
    pub fn run(&self) {
        self.experiment.run();
    }
}