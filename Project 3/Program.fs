open System
open Akka.Actor
open Akka.FSharp
open System.Security.Cryptography

let num_nodes = 10
let num_message = 5
let hash_length = 160
let empty_string = ""
let chord_size =  bigint (2.0**160.0)

let chord_system = System.create "chord-system" (Configuration.load())

type Supervisor_Messge = 
    | Start of nodes: int * messages: int
    | Chord_Created of id: IActorRef
    | Insert_New_Node of id: int
    | Node_Inserted
    | Init_Done
    | Received_Message of no_of_hops: int

type Node_Message =
    | Create_Chord
    | Join_Chord of id: IActorRef
    | Begin_Simulation
    | Stabilize of string
    | Notify of string
    | Route of message: string * hops: int
    | Fix_Fingers
    | Find_Successor of string * string * int
    | Update_Successor of string * int
    | NextsPred
    | Send_Pred
    | Update_Succ_Pred of string

let hash_type = "SHA1"

let create_random_string n = 
    let r = Random()
    let chars = Array.concat([[|'a' .. 'z'|];[|'A' .. 'Z'|];[|'0' .. '9'|]])
    let sz = Array.length chars in
    String(Array.init n (fun _ -> chars.[r.Next sz]))

let hash_string (input: string, algo: string) =
    let hash_bytes = input 
                        |> System.Text.Encoding.UTF8.GetBytes
                        |> HashAlgorithm.Create(algo).ComputeHash
    let hash_string = "0" + 
                        (hash_bytes
                        |> Seq.map (fun c -> c.ToString "x2")
                        |> Seq.reduce (+))
    hash_string

let Chord_Node (mailbox : Actor<_>) =
    let finger_table = Array.create hash_length empty_string
    let mutable predecessor = empty_string
    let mutable next = 1 
    let chord_name = mailbox.Context.Self.Path.Name
    
    let find_closest_preceding_node id =
        let mutable index = hash_length-1

        while finger_table.[index] = empty_string do
            index <- index - 1
        //if we have to crossover
        if chord_name < id then
            while (finger_table.[index] <= chord_name || finger_table.[index] >= id) do
                index <- (index - 1)
        else
            while finger_table.[index] >= id && finger_table.[index] <= chord_name do
                index <- (index - 1)
        if index = -1 then
            chord_name
        else
            finger_table.[index]
                
    let basePath = "akka://chord-system/user/supervisor/"
    let supervisor_ref = select basePath chord_system
    
    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with
        | Create_Chord                              ->       finger_table.[0] <- chord_name
                                                             for i in 1 .. 160 do
                                                                 mailbox.Self <! Fix_Fingers
                                                             supervisor_ref <! Chord_Created(mailbox.Self)
                                                             chord_system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromMilliseconds(100.0), TimeSpan.FromMilliseconds(100.0), mailbox.Self, Fix_Fingers)
                                                             chord_system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromMilliseconds(100.0), TimeSpan.FromMilliseconds(100.0), mailbox.Self, NextsPred)

        | Join_Chord(chord_node)                    ->       chord_node <! Find_Successor(chord_name, chord_name, 0)

        | Find_Successor(source_node, id, index)    ->      let origin_node = select (basePath + source_node) chord_system

                                                            if chord_name = finger_table.[0] && index = 0 then
                                                                finger_table.[0] <- id
                                                                origin_node <! Update_Successor(chord_name, index)
                                                            else
                                                                let successor = finger_table.[0]
                                                                if (successor < chord_name && (id > chord_name || id <= successor)) 
                                                                                                    || (id > chord_name && chord_name <= successor) then
                                                                    origin_node <! Update_Successor(successor, index)
                                                                else
                                                                    let next_node = find_closest_preceding_node(id)
                                                                    let actorRef = select (basePath + next_node) chord_system 
                                                                    actorRef <! Find_Successor(source_node, id, index)

        | Stabilize(succ_pred)                      ->      if succ_pred <> "" then
                                                                if (finger_table.[0] < chord_name && (succ_pred < finger_table.[0] || succ_pred > chord_name)) 
                                                                        || (succ_pred > chord_name && succ_pred < finger_table.[0]) then
                                                                    finger_table.[0] <- succ_pred
        
                                                            let actor_ref = select (basePath + finger_table.[0]) chord_system
                                                            actor_ref <! Notify(chord_name)
                                                            
        | Notify(pred)                              ->      if predecessor = "" || ((pred > chord_name && (pred > predecessor || pred < chord_name)) 
                                                                                                    || (predecessor < pred && pred < chord_name)) then
                                                                predecessor <- pred


        | Fix_Fingers                               ->      next <- next + 1
                                                            if next >= hash_length then
                                                                next <- 1
                                                            let value = (bigint.Parse(chord_name, System.Globalization.NumberStyles.HexNumber) + ((2.0** ((next-1)|>float)) |> bigint)) % chord_size
                                                            let mutable value_str = value.ToString("x2")
                                                            if value_str.Length < 41 then
                                                                value_str <- (String.replicate (41-value_str.Length) "0") + value_str
                                                            mailbox.Self <! Find_Successor(chord_name, value_str, next)

        | NextsPred                                 ->      let actor_ref = select (basePath + finger_table.[0]) chord_system
                                                            actor_ref <! Send_Pred

        | Send_Pred                                 ->      mailbox.Sender() <! Stabilize(predecessor)
                                            

        | Update_Successor(successor, index)        ->      finger_table.[index] <- successor
                                                            if index = 0 then
                                                                for i in 1 .. 160 do
                                                                    mailbox.Self <! Fix_Fingers
                                                                supervisor_ref <! Node_Inserted
                                                                chord_system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromMilliseconds(100.0), TimeSpan.FromMilliseconds(100.0), mailbox.Self, Fix_Fingers)
                                                                chord_system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromMilliseconds(100.0), TimeSpan.FromMilliseconds(100.0), mailbox.Self, NextsPred)

        | Begin_Simulation                          ->      for i in 1 .. num_message do
                                                            //generate random message and hash it
                                                            let randomMsg = create_random_string(50) 
                                                            let hashedMsg = hash_string(randomMsg, hash_type)  

                                                            mailbox.Self <! Route(hashedMsg, -1)

        | Route(hashedMsg, hop)                     ->      let num_hops = hop + 1
                                                            if predecessor <> empty_string && ((chord_name < predecessor && (hashedMsg > predecessor || hashedMsg <= chord_name))  || (hashedMsg > predecessor && hashedMsg <= chord_name)) then
                                                                mailbox.Context.Parent <! Received_Message(num_hops)
                                                            else
                                                                let successor = finger_table.[0]
                                                                let mutable path = ""
                                                                if (hashedMsg > chord_name && hashedMsg <= successor) 
                                                                    || (successor < chord_name && (hashedMsg > chord_name || hashedMsg <= successor)) then
                                                                    path <- basePath + successor
                                                                else
                                                                    let next_node = find_closest_preceding_node(hashedMsg)
                                                                    path <- basePath + next_node 
                    
                                                                let actorRef = select path chord_system 
                                                                actorRef <! Route(hashedMsg, num_hops)

        return! loop()
    }
    loop()

let Supervisor (mailbox : Actor<_>) =
    let mutable node_count = 0
    let mutable message_count = 0
    let mutable chord_nodes = [||]
    let mutable chord_id = Unchecked.defaultof<IActorRef>

    let total_requests = num_nodes * num_message
    printfn "total requests = %i" total_requests
    let mutable node_pointer = 0
    let mutable average_hops = 0.0
    let mutable count = 0

    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with
        | Start(nodes, messages)        ->      let prefix = "chord_node_"
                                                node_count <- nodes
                                                message_count <- messages
                                                chord_nodes <- [|for i in 1 .. nodes -> 
                                                                        spawn mailbox.Context (hash_string(prefix+string(i), hash_type)) Chord_Node|]

                                                //Initializing the Chord
                                                chord_nodes.[0] <! Create_Chord

        | Chord_Created(id)             ->      chord_id <- id
                                                mailbox.Self <! Node_Inserted
                                                
        | Node_Inserted                 ->      if node_pointer < node_count - 1 then
                                                    node_pointer <- node_pointer + 1
                                                    //System.Threading.Thread.Sleep(1000)
                                                    mailbox.Self <! Insert_New_Node(node_pointer)
                                                    if node_pointer = node_count - 1 then
                                                        mailbox.Self <! Init_Done

        | Insert_New_Node(id)           ->      chord_nodes.[id] <! Join_Chord(chord_id)

        | Init_Done                     ->      printfn "Chord ring created"
                                                //System.Threading.Thread.Sleep(600000)
                                                chord_nodes |> Seq.iter (fun chord_node -> 
                                                        chord_node <! Begin_Simulation)

        | Received_Message(hops)        ->      count <- count + 1
                                                average_hops <- average_hops + double hops

                                                // REMOVE
                                                printfn "request converged %i in %i hops" count hops

                                                if count = total_requests then
                                                    //calculate average
                                                    average_hops <- average_hops / double total_requests
                                                    printfn "Average hops for %O nodes and %O requests per node = %O" num_nodes num_message average_hops
                                                    mailbox.Context.System.Terminate() |> ignore
        
        return! loop()
    }
    loop()

[<EntryPoint>]
let main argv =
    let supervisor = spawn chord_system "supervisor" Supervisor
    supervisor <! Start(num_nodes, num_message)
    chord_system.WhenTerminated.Wait()
    0