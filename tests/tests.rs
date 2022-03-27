use tokio::time::{Duration, sleep};
use crate::env_setup::{Node, setup, execute_query};

mod env_setup;

#[tokio::test(flavor = "multi_thread")]
async fn test_queries_and_sequence_paxos() {
    
    // setup two nodes
    let nodes = setup([1,2].to_vec()).await;
    sleep(Duration::from_millis(1000)).await;

    // node ids
    let node_one = nodes[0].id;
    let node_two = nodes[1].id;
    
    // creating a test table and inserting some value using node one
    execute_query(node_one, String::from("CREATE TABLE IF NOT EXISTS test2 (ID INTEGER);")).await.unwrap();
    execute_query(node_one, String::from("INSERT INTO test2 VALUES(1);")).await.unwrap();
    let query_one = execute_query(node_one, String::from("SELECT * FROM test2;")).await.unwrap();
    assert!(query_one == "1"); 

    // getting the same value using node two to see that the log is same on both nodes
    let query_two = execute_query(node_two, String::from("SELECT * FROM test2;")).await.unwrap();
    // satify validity and integrity and termination here
    assert!(query_two == "1"); 

    // updating and getting the value using node two
    execute_query(node_two, String::from("UPDATE test2 SET ID = 2;")).await.unwrap();
    let query_three = execute_query(node_two, String::from("SELECT * FROM test2;")).await.unwrap();
    assert!(query_three == "2");

    // getting update value from node one also
    let query_four = execute_query(node_one, String::from("SELECT * FROM test2;")).await.unwrap();
    // satify uniform agreement here
    assert!(query_four == "2");

    // dropping the table at the end
    execute_query(node_one, String::from("DROP TABLE test2;")).await.unwrap();
    
    // shutdown all nodes
    for node in nodes {
        node.shutdown().await;
    }
}


#[tokio::test(flavor = "multi_thread")]
async fn test_leader_and_ballot_leader_election() {

    // let x: Option<u32> = Some(2);
    // assert_eq!(x.is_some(), true);

    // let x: Option<u32> = None;
    // assert_eq!(x.is_some(), false);

    // start cluster
    let mut nodes: Vec<Node> = setup([4,5,6].to_vec()).await;
    sleep(Duration::from_millis(1000)).await;

    // // find leader
    let mut leader: Option<usize> = None;

    assert_eq!(leader.is_none(), true);

    for (index, node) in nodes.iter().enumerate() {
        if node.is_leader() {
            assert!(leader.is_none()); // we should have only one leader
            leader = Some(index);
        }
    }    

    // a leader exists now
    assert_eq!(leader.is_some(), true); 
    
    // crashing the leader now
    let leader_down = nodes.remove(leader.unwrap());
    leader_down.shutdown().await;
    
    // new leader election occurs
    sleep(Duration::from_millis(1000)).await;

    // new leader should be selected now
    leader = None;
    for (index, node) in nodes.iter().enumerate() {
        if node.is_leader() {
            assert!(leader.is_none()); // we should have only one leader
            leader = Some(index);
        }
    }

    // the new leader exists
    assert_eq!(leader.is_some(), true);

    // get ids of nodes and test if the cluster still works with queries
    let node_one = nodes[0].id;
    let node_two = nodes[1].id;
    
    // creating a table and inseting some value with node one and getting value with node one
    execute_query(node_one, String::from("CREATE TABLE IF NOT EXISTS test3 (ID INTEGER);")).await.unwrap();
    execute_query(node_one, String::from("INSERT INTO test3 VALUES(5);")).await.unwrap();
    let query_one = execute_query(node_one, String::from("SELECT * FROM test3;")).await.unwrap();
    assert!(query_one == "5");

    // getting same value using node 2
    let query_two = execute_query(node_two, String::from("SELECT * FROM test3;")).await.unwrap();
    assert!(query_two == "5");

    // dropping table
    execute_query(node_one, String::from("DROP TABLE test3;")).await.unwrap();

    //shutdown all nodes
    for node in nodes {
        node.shutdown().await;
    }
}
