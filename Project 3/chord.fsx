#time "on"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"
open System
open Akka.Actor
open Akka.FSharp
open System.Security.Cryptography
open System.Collections.Generic

let num_nodes = 10
let num_message = 10
let hash_length = 160
let chord_system = System.create "chord-system" (Configuration.load())
let maxNode:bigint = bigint(2.0 ** 160.0)
let mutable noActorsJoined = 0

type Supervisor_Messge = 
    | Start of nodes:int * messages:int
    | CreateOtherNodes
    | Received_Message of no_of_hops: int

type Node_Message =
    // | Begin_Simulation
    // | Route of message: string * hop: int
    | FindSuccessor of id: string * askerNode: string * purpose: int
    | ChangeSuccessor of successor: string
    | Create
    | Join of node: string
    | Stabilize of predOfSucc: string
    | TellYourPred
    | Notify of n : string
    | CheckPred of state : string
    | TellState
    | FixFingers
    | UpdateFinger of value: string * index: int 

let hash_type = "SHA1"

let hash_string (input: string) =
    let algo = hash_type
    let hash_bytes = input 
                        |> System.Text.Encoding.UTF8.GetBytes
                        |> HashAlgorithm.Create(algo).ComputeHash
    let hash_string = "0" + 
                        (hash_bytes
                        |> Seq.map (fun c -> c.ToString "X")
                        |> Seq.reduce (+))
    hash_string


let ranStr n = 
    let r = Random()
    let chars = Array.concat([[|'a' .. 'z'|];[|'A' .. 'Z'|];[|'0' .. '9'|]])
    let sz = Array.length chars in
    String(Array.init n (fun _ -> chars.[r.Next sz]))

let convertToBigInt hash =
    bigint.Parse(hash, System.Globalization.NumberStyles.HexNumber)

let chordNode (mailbox : Actor<_>) =
    // let mutable finger_table = Array2D.init hash_length 2 (fun _ _ -> "")
    let mutable finger_table = Array.create hash_length ""
    let mutable predecessor = ""
    let currNode = mailbox.Context.Self.Path.Name
    let basePath = "akka://chord-system/user/supervisor/"
    let mutable state = "active"
    let mutable next = 1
    let nodeInt:bigint = convertToBigInt currNode

    let getActorRef id =
        let path = basePath+id
        let actorRef = select path chord_system
        actorRef
   
    let find_closest_preceeding_node id =
        let mutable index = hash_length-1
        while finger_table.[index] = "" do
            index <- (index-1)

        //if we have to crossover
        if id < currNode then
            if finger_table.[index] > currNode then
                printfn "%s --- index %i case 0" currNode index              
                finger_table.[index]
            else
                while (id < finger_table.[index] && finger_table.[index-1]<= finger_table.[index]) do 
                    index <- (index-1)
                    printfn "index %i case 1" index
                if finger_table.[index-1]> finger_table.[index] && id< finger_table.[index] then
                    index <- (index-1)
                    printfn "index %i case 2" index
                finger_table.[index]
        else
            //it has crossed over which we don't want
            if finger_table.[index] < currNode || finger_table.[index] = "" then                
                while finger_table.[index-1] <= finger_table.[index] || index >1 do
                    index <- (index-1)
                    printfn "index %i case 3" index              
                index <- (index-1)
            if index = 0 then
                finger_table.[index]
            else
                while id < finger_table.[index] do
                   index <- (index-1)
                   printfn "index %i case 4" index       
                finger_table.[index]
                    
    
    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with
        | FindSuccessor(id, asker, purpose) ->             
            let successor = finger_table.[0]
            if currNode = successor && purpose = -1 then
                let actorRef = getActorRef asker
                actorRef <! ChangeSuccessor(successor)
                mailbox.Self <! ChangeSuccessor(asker)
            else 
                if (id > currNode && id <= successor) || (successor< currNode && (id> currNode || id<= successor)) then
                    let actorRef = getActorRef asker
                    //means join
                    if purpose = -1 then
                        noActorsJoined <- noActorsJoined+1
                        actorRef <! ChangeSuccessor(successor)                        
                    //means update finger table
                    else 
                        actorRef <! UpdateFinger(id, purpose)                 
                else
                    let cpn = find_closest_preceeding_node id
                    let actorRef = getActorRef cpn
                    actorRef <! FindSuccessor(id, asker, purpose)

        | ChangeSuccessor(successor) ->
            finger_table.[0] <- successor

        | Create ->
            printfn "Create called"
            noActorsJoined <- 1
            predecessor <- ""
            finger_table.[0] <- currNode
            mailbox.Sender() <! CreateOtherNodes

        | Join(existingNode) ->
            printfn "Join Called"
            predecessor <- ""
            let actorRef = getActorRef(existingNode)
            actorRef <! FindSuccessor(currNode, currNode, -1)

        | TellYourPred ->
            // printfn "Calling stabilize"
            mailbox.Sender() <! Stabilize(predecessor)

        | Stabilize(x) ->
            if x <> "" then
                // crossover
                if(finger_table.[0] < currNode) then
                    if x < finger_table.[0] || x > currNode then
                        finger_table.[0] <- x
                else if (x > currNode && x < finger_table.[0]) then
                    finger_table.[0] <- x
            let actorRef = getActorRef finger_table.[0]
            actorRef <! Notify(currNode)

        | Notify(pred) ->
            if (predecessor = "") || (pred> currNode && (pred > predecessor || pred < currNode)) || (pred> predecessor && pred< currNode) then
                predecessor <- pred

        | TellState ->
            mailbox.Sender() <! CheckPred(state)
         
        | CheckPred(state) ->
            if state = "failed" then
                predecessor <- ""
            
        | FixFingers ->
            next <- next + 1
            if next >= hash_length then
                next <- 1         
         
            let power = (next-1) |> float
            let addition = (nodeInt + bigint(2.0 ** power))
            let finger_int_val =  (addition % maxNode)
            let mutable finger_val = finger_int_val.ToString("X")
            if finger_val.Length < 41 then
                finger_val <- (String.replicate (41-finger_val.Length) "0") + finger_val
            mailbox.Self <! FindSuccessor(finger_val, currNode, next)
        
        | UpdateFinger(id, index) ->
            // printfn "Update finger of %i as %s" index id
            finger_table.[index] <- id

        // | Begin_Simulation ->
        //     for i in 1 .. num_message do
        //         //generate random message and hash it
        //         let randomMsg = ranStr(50) 
        //         let hashedMsg = hash_string(randomMsg, hash_type)  
        //         //REMOVE
        //         //if key already in successor
        //         // if (currNode < predecessor && (hashedMsg > predecessor || hashedMsg <= currNode)) || (hashedMsg > predecessor && hashedMsg <= currNode) then
        //             //REMOVE
        //             // printfn "Converging message"
        //             // mailbox.Context.Parent <! Received_Message(0)
        //         // else
        //             //if successor has key
        //         let successor = finger_table.[0]
        //         if (hashedMsg > currNode && hashedMsg <= successor) || (successor< currNode && (hashedMsg> currNode || hashedMsg<= successor)) then
        //             let path = basePath+successor
        //             let actorRef = select path chord_system
        //             mailbox.Context.Parent <! Received_Message(1)
        //             //REMOVE 
        //             // printfn "routing %s to %s" hashedMsg path
        //             // actorRef <! Route(hashedMsg, 0) 
        //         else
        //             let next_node = find_closest_preceeding_node(hashedMsg)
        //             let mutable path = basePath+next_node 
        //             let actorRef = select path chord_system
                    
        //             actorRef <! Route(hashedMsg, 0)


        // | Route(hashedMsg, hop) ->
        //     let mutable num_hops = hop + 1
         
        //     let successor = finger_table.[0]
        //     if (hashedMsg > currNode && hashedMsg <= successor) || (successor< currNode && (hashedMsg> currNode || hashedMsg<= successor)) then
        //         let path = basePath+successor
        //         let actorRef = select path chord_system 
        //         mailbox.Context.Parent <! Received_Message(num_hops+1)

        //         //REMOVE
        //         // printfn "routing %s to SUCCESSOR %s" hashedMsg path
        //         // actorRef <! Route(hashedMsg, num_hops) 
        //     else
        //         let next_node = find_closest_preceeding_node(hashedMsg)
        //         let mutable path = basePath+next_node 
        //         let actorRef = select path chord_system
        //         //REMOVE
        //         // printfn "routing %s to %s" hashedMsg path
        //         // printfn "send key to next closest preceeding node %s %s" hashedMsg next_node
        //         actorRef <! Route(hashedMsg, num_hops)

        chord_system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.0), mailbox.Self, TellYourPred, mailbox.Self)         
        chord_system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.0), mailbox.Self, FixFingers, mailbox.Self)
        return! loop()
    }
    loop()

let Supervisor (mailbox : Actor<_>) =
    let mutable init_count = 0
    let mutable node_count = 0
    // let mutable chord_list = new List<IActorRef>()
    let mutable firstNode = ""

    let total_requests = num_nodes * num_message
    printfn "total requests = %i" total_requests
    let mutable average_hops = 0
    let mutable count = 0
    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with
        | Start(nodes, messages) -> 
            let nodeName = ranStr 10
            let hashedValue = hash_string nodeName
            printfn "First node %s" hashedValue
            firstNode <- hashedValue
            let actorRef = spawn mailbox.Context hashedValue chordNode
            actorRef <! Create
            

        | CreateOtherNodes ->
            for i in 1 .. num_nodes-1 do
                let nodeName = ranStr 10
                let hashedValue = hash_string nodeName
                let actorRef = spawn mailbox.Context hashedValue chordNode
                actorRef <! Join(firstNode)
      
        | Received_Message(hops) ->
            count <- count + 1
            average_hops <- average_hops + hops

            // REMOVE
            printfn "request converged %i in %i hops" count hops

            if count = total_requests then
                //calculate average
                average_hops <- average_hops / total_requests
                printfn "Average hops for %i nodes and %i requests per node = %i" num_nodes num_message average_hops
                mailbox.Context.System.Terminate() |> ignore             

        return! loop()
    }
    loop()

let supervisor = spawn chord_system "supervisor" Supervisor
supervisor <! Start(num_nodes, num_message)
chord_system.WhenTerminated.Wait()
