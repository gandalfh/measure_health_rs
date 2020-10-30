

use std::collections::HashMap;
use std::sync::{RwLock, Mutex, Arc};
use lazy_static::lazy_static;
use std::time::{ Instant, SystemTime };
use std::sync::atomic::{AtomicUsize, Ordering};

struct Rollup {
    pub total: u32,
    pub sample_count: u32,
    pub start_ticks: std::time::Instant,
    pub start_time: std::time::SystemTime,
    pub active: bool,
    pub first: bool,
    pub whole: bool,
}

impl Rollup {
    fn reset_rollup(&mut self) {
        self.total = 0;
        self.sample_count = 0;
        self.start_ticks = Instant::now();
        self.start_time = SystemTime::now();
        self.first = false;
        self.active = false;
        self.whole = false;
    }

    fn add_value(&mut self, value: u32) {
        self.sample_count = self.sample_count + 1;
        self.total += value;
    }

    fn new() -> Rollup {
        Rollup { 
            total: 0,
            sample_count: 0,
            start_ticks: Instant::now(),
            start_time: SystemTime::now(),
            first: false,
            active: false,
            whole: false,
        }
    }

    fn debug_out_measures(&self) {
        println!("[total: {}, sample_count: {}, start_ticks: {} seconds, start_time: {:?}, first: {}, active: {}, whole: {}", self.total, self.sample_count, self.start_ticks.elapsed().as_secs(), self.start_time, self.first, self.active, self.whole);
    }

}

struct Rollups {
    current_rollup_index: usize,
    interval_seconds: u32,
    pub rollups: Vec<Rollup>,
}

impl Rollups {
        fn new(interval_seconds: u32, rollup_count: u32) -> Rollups {
            let mut rollups = Vec::new();
            
        for _ in 0..rollup_count {
            rollups.push(Rollup::new());   
        }
        Rollups { interval_seconds: interval_seconds, current_rollup_index: 0, rollups: rollups }
    }

    fn add_value(&mut self, value: u32) {
        let mut result = self.rollups.get_mut(self.current_rollup_index);
        let mut rollup: &mut Rollup;

        match result {
            Some(rollup_result) => rollup = rollup_result,
            None => panic!("no rollup at index {}", self.current_rollup_index)
        }

        let now = Instant::now();
        
        if now.duration_since(rollup.start_ticks).as_secs() > self.interval_seconds.into() {
            self.current_rollup_index = (self.current_rollup_index + 1) % self.rollups.len();

            result = self.rollups.get_mut(self.current_rollup_index);

            match result {
                Some(rollup_result) => {
                    rollup = rollup_result;
                    rollup.reset_rollup();
                }
                None => panic!("no rollup at index {}", self.current_rollup_index)
            }
        }            
        rollup.add_value(value);
    }

    fn debug_out_measures(&self) {
        println!("current_rollup_index: {}, interval_seconds: {}", self.current_rollup_index, self.interval_seconds);
        for i in 0..self.rollups.len() {
            match self.rollups.get(i) {
                Some(rollup) => 
                    if rollup.sample_count > 0 
                    { 
                        println!("rollup: {}", i);
                        rollup.debug_out_measures() 
                    },
                None => panic!("no rollup at index {}", i)
            }
        }
    }
}

struct RollupIntervals {
    rollup_intervals: Vec<Rollups>,
}

impl RollupIntervals {
    fn new() -> RollupIntervals {
        let mut rollup_intervals = Vec::new();
        rollup_intervals.push(Rollups::new(60, 60));
        rollup_intervals.push(Rollups::new(60*60, 24));
        rollup_intervals.push(Rollups::new(60*60*24, 14));

        RollupIntervals {rollup_intervals: rollup_intervals}
    }
    fn add_value(&mut self, value: u32) {
        for i in 0..self.rollup_intervals.len() {
            match self.rollup_intervals.get_mut(i) {
                Some(rollup) => rollup.add_value(value),
                None => panic!("No rollups object found at index {}", i)
            }
        }
    }
    fn debug_out_measures(&self) {
        for i in 0..self.rollup_intervals.len() {
            match self.rollup_intervals.get(i) {
                Some(rollup) => rollup.debug_out_measures(),
                None => panic!("No rollups object found at index {}", i)
            }
        }
    }
}

struct MeasureInner {
    intervals: RollupIntervals,
    name: String
}

impl MeasureInner {
    fn new(name: &String) -> MeasureInner {
        MeasureInner { name: name.clone(), intervals: RollupIntervals::new() }
    }

    fn add_value(&mut self, value: u32) {
        self.intervals.add_value(value);
    }

    fn debug_out_measures(&self) {
        println!("Measure Named: {} ", self.name);
        self.intervals.debug_out_measures();
    }
}

pub struct Measure {
    inner: Mutex<MeasureInner>,
}

impl Measure {
    fn new(name: &String) -> Measure {
        Measure { inner: Mutex::new(MeasureInner::new(name))}
    }

    fn add_value(&self, value: u32) {
        self.inner.lock().unwrap().add_value(value);
    }

    fn debug_out_measures(&self) {
        self.inner.lock().unwrap().debug_out_measures();
    }
}

unsafe impl Sync for Measure {}

pub type MeasureHandle = usize;
static NEXT_MEASURE_HANDLE: AtomicUsize = AtomicUsize::new(1);


struct MeasuresInner {
    measures: HashMap<MeasureHandle, Arc<Measure>>,
    measures_by_name: HashMap<String, MeasureHandle>,
}

impl MeasuresInner {
    fn new() -> MeasuresInner {
        MeasuresInner { measures: HashMap::new(), measures_by_name: HashMap::new() }
    }

    fn new_measure(&mut self, name: &String) -> MeasureHandle {
        if !self.measures_by_name.contains_key(name) {
            let handle = NEXT_MEASURE_HANDLE.fetch_add(1, Ordering::SeqCst);
            self.measures_by_name.insert(name.clone(), handle);
            self.measures.insert(handle, Arc::new(Measure::new(name)));
        }
        match self.measures_by_name.get(name) {
            Some(handle) => *handle,
            None => panic!("Couldn't find measure when it should have been there: {}", name)
        }
    }

    fn exists(&self, name: &String) -> bool {
        self.measures_by_name.contains_key(name)
    }
    fn get_handle(&self, name: &String) -> MeasureHandle {
        self.measures_by_name[name]
    }

    fn add_value(&mut self, handle: MeasureHandle, value: u32) {
        match self.measures.get_mut(&handle) {
            Some(measure) => measure.add_value(value),
            None => {
                let unknown_measure_name = "measure_rollup::unknown_measure_handle_specified";
                let unknown_measure_handle = self.new_measure(&unknown_measure_name.to_string());
                match self.measures.get_mut(&unknown_measure_handle) {
                    Some(unknown_measure) => unknown_measure.add_value(1),
                    None => panic!("Measure named {} that was just created couldn't be found!", unknown_measure_name)
                }
            }
        }
    }

    fn add_value_by_name(&mut self, name: String, value: u32) {
        let measure_handle;
        {
            match self.measures_by_name.get(&name) {
                Some(handle) => measure_handle = *handle,
                None => {
                    let unknown_measure_name_name = "measure_rollup::unknown_measure_name_specified";
                    let unknown_measure_name = self.new_measure(&unknown_measure_name_name.to_string());
                    match self.measures.get_mut(&unknown_measure_name) {
                        Some(unknown_measure) => unknown_measure.add_value(1),
                        None => panic!("Measure named {} that was just created couldn't be found!", unknown_measure_name_name)
                    }
                    return;
                }
            }
        }

        self.add_value(measure_handle, value);
    }

    fn debug_out_measures(&self) {
        println!("debug_out_measures called");
        for (_, measure) in &self.measures {
            measure.debug_out_measures();
        }
    }
}

pub struct Measures {
    inner: RwLock<MeasuresInner>,
}

impl Measures {
    pub fn new() -> Measures {
        Measures { inner: RwLock::new(MeasuresInner::new()) }
    }
    
    pub fn new_measure(&self, name: &String) -> MeasureHandle {
        let exists;
        {
            let measures_inner = self.inner.read().unwrap();
            exists = measures_inner.exists(name);
        }

        if !exists {
            let mut measures_inner = self.inner.write().unwrap();
            measures_inner.new_measure(name)
        }
        else {
            let measures_inner = self.inner.read().unwrap();
            measures_inner.get_handle(name)
        }
    }

    pub fn add_value(&self, handle: MeasureHandle, value: u32) {
        let write = self.inner.write();
        let mut measures_inner = write.unwrap();
        measures_inner.add_value(handle, value);
    }

    pub fn get_measure(&self, handle: MeasureHandle) -> Option<Arc<Measure>> {
        let measures_inner = self.inner.write().unwrap();
        match measures_inner.measures.get(&handle) {
            Some(result) => Some(result.clone()),
            None => Option::None
        }
    }
 
    pub fn add_value_by_name(&self, name: String, value: u32) {
        let mut measures_inner = self.inner.write().unwrap();
        measures_inner.add_value_by_name(name, value);
    }

    pub fn debug_out_measures(&self) {
        self.inner.read().unwrap().debug_out_measures();
    }

}

lazy_static! {
    static ref MEASURES: Measures = Measures::new();
}

pub fn measures() -> &'static Measures {
    &MEASURES
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    #[test]
    fn test_rollup_reset() {
        let mut rollup: Rollup = Rollup{
            total: 5,
            sample_count: 5,
            start_ticks: std::time::Instant::now(),
            start_time: std::time::SystemTime::now(),
            active: true,
            first: true,
            whole: true,            
        };
        assert_eq!(rollup.total, 5);
        rollup.reset_rollup();
        assert_eq!(rollup.total, 0);
    }

    #[test]
    fn test_rollups() {
        let mut rollups = RollupIntervals::new();
        child_rollup_function(&mut rollups);
    }

    fn child_rollup_function(rollups: &mut RollupIntervals) {
        rollups.add_value(5);
    }

    #[test]
    fn test_measure() {

        {
            let measure_handle;
            {
                measure_handle = MEASURES.new_measure(&"Water Meter 1".to_string());
            }

            MEASURES.add_value(measure_handle, 45);
        }

        {
            let measure_handle;
            {
                measure_handle = MEASURES.new_measure(&"Water Meter 2".to_string());
            }

            MEASURES.add_value(measure_handle, 45);
        }

        {
            let measure_handle;
            {
                measure_handle = MEASURES.new_measure(&"Water Meter 1".to_string());
            }

            MEASURES.add_value(measure_handle, 45);
        }

        MEASURES.debug_out_measures();
    }

    #[test]
    fn test_multi_thread() {
        let mut threads:Vec<std::thread::JoinHandle<()>> = Vec::new();

        for i in 0..100 {
            if i % 2 > 0 {
                threads.push(thread::spawn(test_thread1));
            }
            else {
                threads.push(thread::spawn(test_thread2));
            }
        }

        MEASURES.debug_out_measures();

        for handle in threads {
            handle.join().unwrap();
        }

        let max_handle = MEASURES.new_measure(&"Guardian".to_string());

        assert_eq!(max_handle, 1001);

        MEASURES.debug_out_measures();
    }

    fn test_thread1() {
        let mut handles:Vec<MeasureHandle> = Vec::new();
        for i in 0..1000 {
            let name = (format!("Water Meter {}", i)).to_string();
            handles.push(MEASURES.new_measure(&name));
        }

        for i in 0..handles.len() {
            match MEASURES.get_measure(handles[i]) {
                Some(measure) => measure.add_value(32),
                None => panic!("Measure should have been found by handle, something seriously wrong with code or test!")
            }

            
        }
    }
    fn test_thread2() {
        let mut handles:Vec<MeasureHandle> = Vec::new();
        for i in 0..1000 {
            let name = (format!("Water Meter {}", i)).to_string();
            handles.push(MEASURES.new_measure(&name));
        }

        for i in 0..handles.len() {
            MEASURES.add_value(handles[i], 32);
        }
    }

}
