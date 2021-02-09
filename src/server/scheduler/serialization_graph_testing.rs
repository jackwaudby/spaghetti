// use crate::server::scheduler::serialization_graph_testing::node::{EdgeType, Node, State};
// use crate::server::scheduler::Scheduler;
// use crate::Result;

// use std::collections::HashSet;
// use std::sync::RwLock;

pub mod node;

pub mod error;

// /// Serialization Graph.
// ///
// /// Contains a fixed number of `Node`s each wrapped in a `RwLock`.
// ///
// /// # Safety
// /// Graph can have multiple owners and shared across thread boundaries.
// /// Each `Node` is wrapped in a `RwLock`.
// #[derive(Debug)]
// pub struct SerializationGraphTesting {
//     nodes: Vec<RwLock<Node>>,
//     /// Handle to storage layer.
//     pub data: Arc<Workload>,
// }

// impl Scheduler for SerializationGraphTesting {
//     /// Register a transaction with the serialization graph.
//     pub fn register(&self) -> Result<()> {
//         // Find free node in graph.
//         for node in self.nodes {
//             // Take exculsive access of node.
//             let node = node.write().unwrap();
//             if node.get_status() == Status::Free {
//                 node.set_status(Status::Active);
//                 let slot = Some(node.id);
//             } else {
//                 continue;
//             }
//         }

//         None
//     }

//     pub fn commit(&self, this_node_id: usize) -> Result<()> {
//         let this_node = self.nodes[this_node_id].write().unwrap(); // take write lock
//         let status = this_node.get_status(); // get status
//         let incoming = this_node.has_incoming(); // get incoming

//         match status {
//             Status::Active => {
//                 if incoming {
//                     Err(SpaghettiError(ErrorKind::Delay))
//                 } else {
//                     this_node.set_status(Status::Committed);
//                     Ok(())
//                 }
//             }
//             Status::Aborted => Err(SpaghettiError(ErrorKind::SerializableError)),
//             Status::Free => panic!("trying to commit a free sloted"),
//             Status::Committed => panic!("trying to commit an already committed transaction"),
//         }
//     }
// }

// impl SerializationGraphTesting {
//     /// Initialise serialization graph with `size` nodes.
//     pub fn new(size: i32) -> Self {
//         info!("initialise serialization graph with {} nodes", size);
//         let mut nodes = vec![];
//         for i in 0..size {
//             let node = RwLock::new(Node::new(i as usize));
//             nodes.push(node);
//         }
//         SerializationGraphTesting { nodes }
//     }

//     /// get node from the serialization graph
//     pub fn get_node(&self, id: usize) -> &RwLock<Node> {
//         &self.nodes[id]
//     }

//     /// Insert an edge into the serialization graph
//     ///
//     /// `(from_node)--->(this_node)`
//     ///
//     /// Returns `()` if insertion leaves the graph acyclic (edge is inserted).
//     /// Returns `SerializableError` if insertion would violate serializability (edge is not inserted).
//     pub fn add_edge(&self, from_node: usize, this_node: usize, rw_edge: bool) -> Result<()> {
//         // acquire read locks on nodes
//         let this_node = self.nodes[this_node].read().unwrap();
//         let from_node = self.nodes[from_node].read().unwrap();

//         let from_node_status: Status = from_node.get_status();

//         match from_node_status {
//             Status::Aborted => match rw_edge {
//                 false => {
//                     info!("from_node already aborted, edge is ww or wr so abort this_node");
//                     Err(SpaghettiError(ErrorKind::SerializableError))
//                 }
//                 /* from_node aborted, attempting to add ww/wr edge so abort transaction */
//                 true => {
//                     info!("from_node already aborted, edge is rw so skip add edge");
//                     Ok(())
//                 } /* from_node aborted, but attempting to add rw edge so skip over */
//             },
//             Status::Committed => {
//                 info!("skip add edge, from_node already committed");
//                 Ok(())
//             } /* committed so skip error */
//             Status::Active => {
//                 /* from_node active so add edge */
//                 if from_node.id != this_node.id {
//                     info!("add ({})->({})", from_node.id, this_node.id);
//                 } else {
//                     info!("self edge, so skip add edge");
//                 }

//                 from_node.insert_edge(this_node.id, EdgeType::Outgoing);
//                 this_node.insert_edge(from_node.id, EdgeType::Incoming);
//                 Ok(())
//             }
//             Status::Free => panic!("node should not be free {:#?}, {:#?}", this_node, from_node),
//         }
//     }

//     pub fn reduced_depth_first_search(&self, current_node: usize) -> Result<()> {
//         let mut stack = Vec::new(); /* tracking nodes to visit */
//         let mut visited = HashSet::new(); /* tracking visited nodes */
//         {
//             let current_node = self.nodes[current_node].read().unwrap(); /* get read lock on current_node */
//             stack.append(&mut current_node.get_outgoing()); /* push children to stack */
//             visited.insert(current_node.id); /* mark as visited */
//         } /* drop read lock on current_node */
//         while let Some(current_node_id) = stack.pop() {
//             visited.insert(current_node_id); // mark as visited
//             let current_node = self.nodes[current_node_id as usize].read().unwrap(); // get read lock on current_node
//             if current_node.status.lock().unwrap().clone() == Status::Active {
//                 for child in current_node.get_outgoing() {
//                     if visited.contains(&child) {
//                         // return early
//                         return Err(SpaghettiError(ErrorKind::SerializableError));
//                     } else {
//                         let c = self.nodes[child as usize].read().unwrap(); // get read lock on child_node
//                         stack.append(&mut c.get_outgoing());
//                     }
//                 }
//             }
//         }
//         Ok(())
//     }
// }
