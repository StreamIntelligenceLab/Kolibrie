use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::mpsc::Receiver;
use std::thread;
#[cfg(not(test))]
use log::{info, warn, trace, debug, error}; // Use log crate when building application
#[cfg(test)]
use std::{println as info, println as warn, println as trace, println as debug, println as error};
use std::fmt::Debug;
use std::hash::Hash;
use rsp::s2r::{ContentContainer, CSPARQLWindow, Report, ReportStrategy, Tick, WindowTriple};
use rsp::r2s::{Relation2StreamOperator, StreamOperator};
use rsp::r2r::{R2ROperator};
use shared::triple::Triple;

use crate::execute_query::execute_query;
use crate::sparql_database::SparqlDatabase;
pub struct Syntax{

}
pub enum OperationMode{
    SingleThread, MultiThread
}
pub struct  RSPBuilder<'a, I, O> {
    width: usize,
    slide: usize,
    tick: Option<Tick>,
    report_strategy: Option<ReportStrategy>,
    triples: Option<&'a str>,
    rules: Option<&'a str>,
    query_str: Option<&'a str>,
    result_consumer: Option<ResultConsumer<O>>,
    r2s: Option<StreamOperator>,
    r2r: Option<Box<dyn R2ROperator<I, O, O>>>,
    operation_mode : OperationMode,

    syntax: String
}
impl <'a, I, O> RSPBuilder<'a, I, O> where O: Clone + Hash + Eq + Send + Debug +'static, I: Eq + PartialEq + Clone + Debug + Hash + Send +'static{
    pub fn new(width: usize, slide: usize)-> RSPBuilder<'a, I, O>{
        RSPBuilder{
            width,
            slide,
            tick: None,
            report_strategy: None,
            triples: None,
            rules: None,
            query_str: None,
            result_consumer: None,
            r2s: None,
            r2r: None,
            operation_mode: OperationMode::MultiThread,
            syntax: "ntriples".to_string(),
        }
    }
    pub fn add_tick(mut self, tick: Tick)->RSPBuilder<'a, I, O>{
        self.tick=Some(tick);
        self
    }
    pub fn add_report_strategy(mut self, strategy: ReportStrategy)->RSPBuilder<'a, I, O>{
        self.report_strategy= Some(strategy);
        self
    }
    pub fn add_triples(mut self, triples: &'a str)->RSPBuilder<'a, I, O>{
        self.triples= Some(triples);
        self
    }
    pub fn add_rules(mut self, rules: &'a str)->RSPBuilder<'a, I, O>{
        self.rules= Some(rules);
        self
    }
    pub fn add_query(mut self, query: &'a str)->RSPBuilder<'a, I, O>{
        self.query_str= Some(query);
        self
    }
    pub fn add_consumer(mut self, consumer: ResultConsumer<O>)->RSPBuilder<'a, I, O>{
        self.result_consumer= Some(consumer);
        self
    }
    pub fn add_r2s(mut self, r2s: StreamOperator)->RSPBuilder<'a, I, O>{
        self.r2s= Some(r2s);
        self
    }
    pub fn add_r2r(mut self, r2r: Box<dyn R2ROperator<I, O, O>>) ->RSPBuilder<'a, I, O>{
        self.r2r= Some(r2r);
        self
    }
    // pub fn add_syntax(mut self, syntax: Syntax)->RSPBuilder<'a, I, O>{
    //     self.syntax = Some(syntax);
    //     self
    // }
    pub fn set_operation_mode(mut self, operation_mode: OperationMode)->RSPBuilder<'a, I, O>{
        self.operation_mode = operation_mode;
        self
    }
    pub fn build(self) -> RSPEngine<I,O>{
        RSPEngine::new(
            self.width,
            self.slide,
        self.tick.unwrap_or_default(),
        self.report_strategy.unwrap_or_default(),
        self.triples.unwrap_or(""),
        self.syntax,
        self.rules.unwrap_or(""),
        self.query_str.expect("Please provide R2R query"),
        self.result_consumer.unwrap_or(ResultConsumer{function: Arc::new( Box::new(|r|println!("Bindings: {:?}",r)))}),
            self.r2s.unwrap_or_default(),
            self.r2r.expect("Please provide R2R operator!"),
            self.operation_mode
        )

    }
}
pub struct RSPEngine<I,O> where I: Eq + PartialEq + Clone + Debug + Hash + Send{
    s2r: CSPARQLWindow<I>,
    r2r: Arc<Mutex<Box<dyn R2ROperator<I,O, O>>>>,
    r2s_consumer: ResultConsumer<O>,
    r2s_operator: Arc<Mutex<Relation2StreamOperator<O>>>
}
pub struct ResultConsumer<I>{
    pub function: Arc<dyn Fn(I) ->() + Send + Sync>
}


impl  <I, O> RSPEngine<I, O> where O: Clone + Hash + Eq + Send +'static, I: Eq + PartialEq + Clone + Debug + Hash + Send +'static{

    pub fn new(width: usize, slide: usize, tick: Tick, report_strategy: ReportStrategy, triples: &str, syntax: String, rules: &str, query_str: &str, result_consumer: ResultConsumer<O>, r2s: StreamOperator, r2r: Box<dyn R2ROperator<I, O, O>>, operation_mode: OperationMode) -> RSPEngine<I, O>{
        let mut report = Report::new();
        report.add(report_strategy);
        let mut window = CSPARQLWindow::new(width, slide, report, tick);
        let mut store = r2r;

        match store.load_triples(triples, syntax){
            Err(parsing_error)=>error!("Unable to load ABox: {:?}", parsing_error.to_string()),
            _ => ()
        }
        store.load_rules(rules);
        // let query = match Query::parse(query_str, None){
        //     Ok(parsed_query) => parsed_query,
        //     Err(err)=>{
        //         error!("Unable to parse query! {:?}", err.to_string());
        //         error!("Using Select * WHERE{{?s ?p ?o}} instead");
        //         Query::parse("Select * WHERE{?s ?p ?o}", None).unwrap()
        //     }
        // };
        let mut engine = RSPEngine{s2r: window, r2r:  Arc::new(Mutex::new(store)), r2s_consumer: result_consumer, r2s_operator: Arc::new(Mutex::new(Relation2StreamOperator::new(r2s,0)))};
        match operation_mode {
            OperationMode::SingleThread => {
                let consumer_temp = engine.r2r.clone();
                let r2s_consumer = engine.r2s_consumer.function.clone();
                let mut r2s_operator = engine.r2s_operator.clone();
                let query_str_cpy = query_str.clone().to_string();
                let call_back: Box<dyn FnMut(ContentContainer<I>) -> ()> = Box::new(move |content| {
                    Self::evaluate_r2r_and_call_r2s(query_str_cpy.as_str(), consumer_temp.clone(), r2s_consumer.clone(), r2s_operator.clone(), content);
                });
                engine.s2r.register_callback(call_back);
                error!("Unsupported operation (single thread processing)!")
            },
            OperationMode::MultiThread => {
                let consumer = engine.s2r.register();
                engine.register_r2r(consumer, query_str);
            }
        }


        engine
    }
    fn register_r2r(&mut self,receiver: Receiver<ContentContainer<I>>, query: &str){
        let consumer_temp = self.r2r.clone();
        let r2s_consumer = self.r2s_consumer.function.clone();
        let mut r2s_operator = self.r2s_operator.clone();
        let query = query.to_string();
        thread::spawn(move||{
            loop{
                match receiver.recv(){
                    Ok(mut content)=> {
                        Self::evaluate_r2r_and_call_r2s(&query, consumer_temp.clone(), r2s_consumer.clone(), r2s_operator.clone(), content);
                    },
                    Err(_) => {
                        debug!("Shutting down!");
                        break;
                    }
                }
            }
            debug!("Shutdown complete!");
        });
    }

    fn evaluate_r2r_and_call_r2s(query: &str, consumer_temp: Arc<Mutex<Box<dyn R2ROperator<I, O, O>>>>, r2s_consumer: Arc<dyn Fn(O) + Send + Sync>, mut r2s_operator: Arc<Mutex<Relation2StreamOperator<O>>>, mut content: ContentContainer<I>) {
        debug!("R2R operator retrieved graph {:?}", content);
        let time_stamp = content.get_last_timestamp_changed();
        let mut store = consumer_temp.lock().unwrap();
        content.clone().into_iter().for_each(|t| {
            store.add(t);
        });
        let inferred = store.materialize();
        let r2r_result = store.execute_query(&query);
        let r2s_result = r2s_operator.lock().unwrap().eval(r2r_result, time_stamp);
        // TODO run R2S in other thread
        for result in r2s_result {
            (r2s_consumer)(result);
        }
        //remove data from stream
        content.iter().for_each(|t| {
            store.remove(t);
        });
        inferred.iter().for_each(|t|{
            store.remove(t);
        });
    }


    pub fn add(&mut self, event_item: I, ts: usize) {
        self.s2r.add_to_window(event_item,ts);
    }
    pub fn stop(&mut self){
        self.s2r.stop();
    }
    pub fn parse_data(&mut self, data: &str) -> Vec<I>{
        self.r2r.lock().unwrap().parse_data(data)
    }
}

pub struct SimpleR2R {
    pub item: SparqlDatabase
}
impl R2ROperator<Triple,Vec<String>, Vec<String>> for SimpleR2R {
    fn load_triples(&mut self, data: &str, syntax: String) -> Result<(), String> {
        // let reseult = self.item.load_triples(data,syntax);
        // println!("Store size after loading: {:?}", self.item.triple_index.len());
        // reseult
        error!("Unsupported operation");
        Err("something went wrong".to_string())

    }

    fn load_rules(&mut self, data: &str) -> Result<(), &'static str> {
        error!("Unsupported operation load rules");

        //self.item.load_rules(data)
        Err("something went wrong")
    }

    fn add(&mut self, data: Triple) {
        self.item.add_triple(data);
    }

    fn remove(&mut self, data: &Triple) {
        self.item.delete_triple(data);
    }

    fn materialize(&mut self) -> Vec<Triple>{
        // println!("Store size: {:?}", self.item.triple_index.len());
        // let inferred = self.item.materialize();
        // inferred.into_iter().map(|t|WindowTriple{s:Encoder::decode(&t.s.to_encoded()).unwrap().to_string(),
        // p:Encoder::decode(&t.p.to_encoded()).unwrap().to_string(),
        // o:Encoder::decode(&t.o.to_encoded()).unwrap().to_string()}).collect()
        error!("Unsupported operation materialize");
        Vec::new()
    }

    fn execute_query(&mut self, query: &str) -> Vec<Vec<String>> {
        let results = execute_query(query, &mut self.item);
        results
    }

    fn parse_data(&mut self, data: &str) -> Vec<Triple> {
       self.item.parse_and_encode_ntriples(data)
    }
}

#[cfg(test)]
mod tests{

    use std::time::Duration;
    use super::*;

    #[test]
    #[ignore]
    fn rsp_integration(){
        let result_container = Arc::new(Mutex::new(Vec::new()));
        let result_container_clone = Arc::clone(&result_container);
        let window_size = 10;
        let function = Box::new(move |r| {
            println!("Bindings: {:?}",r);
            result_container_clone.lock().unwrap().push(r);
        });
        let result_consumer = ResultConsumer{function: Arc::new(function)};
        let mut r2r = Box::new(SimpleR2R {item: SparqlDatabase::new()});
        let mut engine = RSPBuilder::new(window_size,2)
            .add_tick(Tick::TimeDriven)
            .add_report_strategy(ReportStrategy::OnWindowClose)
            .add_query("SELECT ?s WHERE{ ?s a <http://www.w3.org/test/SuperType>}")
            .add_consumer(result_consumer)
            .add_r2r(r2r)
            .add_r2s(StreamOperator::RSTREAM)
            .build();
        for i in 0..20 {
            let data = format!("<http://test.be/subject{}> a <http://www.w3.org/test/SuperType> .", i);
            let triples = engine.parse_data(&data);
            for triple in triples{
                engine.add(triple,i);
            }
        }
        engine.stop();
        thread::sleep(Duration::from_secs(2));
        assert_eq!(result_container.lock().unwrap().len(),8*window_size);
    }
    #[test]
    fn rsp_integration_single_thread(){
        let result_container = Arc::new(Mutex::new(Vec::new()));
        let result_container_clone = Arc::clone(&result_container);
        let window_size = 10;
        let function = Box::new(move |r| {
            println!("Bindings: {:?}",r);
            result_container_clone.lock().unwrap().push(r);
        });
        let result_consumer = ResultConsumer{function: Arc::new(function)};
        let mut r2r = Box::new(SimpleR2R {item: SparqlDatabase::new()});
        let mut engine = RSPBuilder::new(window_size,2)
            .add_tick(Tick::TimeDriven)
            .add_report_strategy(ReportStrategy::OnWindowClose)
            .add_query("SELECT ?s WHERE{ ?s a <http://www.w3.org/test/SuperType>}")
            .add_consumer(result_consumer)
            .add_r2r(r2r)
            .add_r2s(StreamOperator::RSTREAM)
            .set_operation_mode(OperationMode::SingleThread)
            .build();
        for i in 0..20 {
            let data = format!("<http://test.be/subject{}> a <http://www.w3.org/test/SuperType> .", i);
            let triples = engine.parse_data(&data);
            for triple in triples{
                engine.add(triple,i);
            }
        }
        engine.stop();
        assert_eq!(result_container.lock().unwrap().len(),8*window_size);
    }

}