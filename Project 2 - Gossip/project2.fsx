#time "on"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"
open System
open Akka.Actor
open Akka.FSharp

type SupervisorMsg = 
    | Start of string
    | Converged of string

type ActorMsg =
    | Initalize of allActors: IActorRef[] * index: int
    | NeighborConverged of IActorRef

// function getNeighbor (allActors: IActorRef[], index: int, topology:string)
    //match topolgy and return neighbors IActorRef[] accordingly
  
//get number of nodes, topology, algo from command line
// Round off to proper cubes for 3D and imperfect 3D
let timer = System.Diagnostics.Stopwatch()
let roundOffNodes (numNode:int) =
    //CHANGE THIS FOR 3D
    let mutable sqrtVal = numNode |> float |> sqrt |> int
    if sqrtVal*sqrtVal <> numNode then
        sqrtVal <- sqrtVal + 1
    sqrtVal*sqrtVal

// Input from Command Line
let mutable nodes = fsi.CommandLineArgs.[1] |> int
let topology = fsi.CommandLineArgs.[2]
let algo = fsi.CommandLineArgs.[3]
let rand = Random(nodes)
if topology = "3D" || topology = "Imp3D" then
    nodes <- roundOffNodes nodes
//Make topology

//Gossip Actors
    //variables neighbourList, numberGossipReceived, gossipReceived bool, int neighbourcount
    //Initialize
        //initialize neighbours list according to topology
        //set numberGossipReceived to 0
    //Gossip
        //ifgossipReceived = false make true
        //increment numberGossipReceived
        //if numberGossipReceived=10
            //send Converged to parent
            //send Converged to Neighbours    
    //NeighborConverged
        //remove actor from list of neighbours as converged
        //decrement neighborCount and check if 0
        //send Converged to Parent
    
    //ifgossipReceived
        //keep sending gossip to random nodes in neighbours list

//PushSum Actors
    //variables neighbourList, pushSumRecieved bool, int neighbourcount, int sum, int weight, int oldRatio, int lessthandelta
    //Initialize
        //initialize neighbours list according to topology
        //set lessthandelta = 0
        //set sum = i
        //set weight to 1
    //PushSum
        //pushSumRecieved = false make true
        //sum += newSum and weight+= newWeight
        //if |oldRatio - newRatio|<10**-10
            //increment lessthandelta
        //else
            //lessthandelta=0
        //if lessthandelta=3 then        
            //send Converged to parent
            //send Converged to Neighbours    
    //NeighborConverged
        //remove actor from list of neighbours as converged
        //decrement neighborCount and check if 0
        //send Converged to Parent

    //ifPushSumReceived
        //keep sending pushSum to random nodes in neighbours list (s/2,w/2)



//Supervisor
let Supervisor (mailbox:Actor<_>) =
    let mutable converged = 0
    //Start
        //check topology
        //create actor pool for numberOfNodes
        //Initialize all actors (with their neighbors according to topology)
        //Start Timer
        //Send gossip to first node
    //Converged
        //increment number of actors that converged
        //if equal to total nodes
        //stop timer and terminate

//Spawn supervisor and start
//let boss = spawn system "boss" BossActor
//boss <! Start("start")
//system.WhenTerminated.Wait()


        
