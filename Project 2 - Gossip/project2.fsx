//get number of nodes, topology, algo from command line

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
    //Converged
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
    //Converged
        //remove actor from list of neighbours as converged
        //decrement neighborCount and check if 0
        //send Converged to Parent

    //ifPushSumReceived
        //keep sending pushSum to random nodes in neighbours list (s/2,w/2)



//Supervisor
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


        
