#time "on"
#r "nuget: Akka"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#r "nuget: FSharp.Json"

open System
open Akka.Actor
open Akka.FSharp
open FSharp.Json


type Api_Message = {
    operation: string
    data: string
}

type Operation_Start = {
    user_pref: string
    num_users: int
    port: int
}

type Register_Op = {
    next_id: int
}

type AckUserReg_Op = {
    user_id: string
    message: string
}

type AckOnline_Op = {
    user_id: string
}


let rand_num = Random()

//Method to fetch machine ip address
let find_machine_ip = 
    let list = System.Net.Dns.GetHostEntry(System.Net.Dns.GetHostName()).AddressList
    let mutable ip_addr = "localhost"
    for i = list.Length - 1 downto 0 do
        if (list.[i].AddressFamily.ToString() = "InterNetwork") then
            ip_addr <- list.[i].ToString()
    printfn "%s" ip_addr
    ip_addr

//Method to Shuffle a List
let swap a b (list: _[]) =
    let tmp = list.[a]
    list.[a] <- list.[b]
    list.[b] <- tmp

let shuffle list =
    Array.iteri (fun i _ -> swap i (rand_num.Next(i, Array.length list)) list) list

//Find Maximum
let get_max x y = 
    if x > y then x
    else y


let hashTagsList = ["cybermonday"; "blackfriday"; "omicron"; "vaccines"; "joebiden"; "climatechange"; "gigiHadid"; "sellingSunset"; "dogsOfTwitter"; "powerpuffGirls"; "weLovePizza"]


let ip_addr = find_machine_ip
let port = fsi.CommandLineArgs.[1] |> string
let prefix = fsi.CommandLineArgs.[2] |> string
let users = fsi.CommandLineArgs.[3] |> string
let server_ip = find_machine_ip
// let server_ip = fsi.CommandLineArgs.[4] |> string
let server_port = "3536"

let configuration = 
    Configuration.parse(
        @"akka {            
            stdout-loglevel : DEBUG
            loglevel : ERROR
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            }
            remote.helios.tcp {
                transport-protocol = tcp
                port = "+port+"
                hostname = "+ip_addr+"
            }
    }")

let tweet_client_system = System.create "TwitterSimulator" configuration

let logger_actor (mailbox:Actor<_>) = 
    let rec loop () = actor {
        let! message = mailbox.Receive()
        printfn "%O" message
        return! loop()
    }
    loop()
let printerRef = spawn tweet_client_system "Logger" logger_actor

type Supervisor_Message = 
    | Start of int*int*int*string
    | Received of int
    | Offline of string
    | Signup of int

type Follow_Message = 
    | Init of list<string>*int

type Client_Message = 
    | Ready
    | Tweet
    | Tasks
    | Login
    | Logoff


let user_actor (curr_id:string) (ref_server:ActorSelection) (num_users:int) (client_id:string) (hash_tags_list:List<String>) (duration:int32) (mailbox:Actor<_>) = 

    let get_random = Random()
    let mutable total_tweets = 0
    let mutable hashtags = []
    let mutable user_online = false


    let rec loop () = actor {
        let! message = mailbox.Receive() 
        match message with
        | Ready ->                                              
                                                                hashtags <- hash_tags_list
                                                                user_online <- true
                                                                tweet_client_system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(50.0), mailbox.Self, Tasks)
                                                                tweet_client_system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(100.0), mailbox.Self, Tweet)
        | Tweet ->                                          
                                                                if user_online then
                                                                    let tweet_actions = [|"Simple_Tweet"; "Hash_Tweet"; "Ment_Hash_Tweet"; "Retweet"|]
                                                                    let rand_val = Random()
                                                                    let random_tweet_action = tweet_actions.[rand_val.Next(tweet_actions.Length)]
                                                                    let curr_time = DateTime.Now
    
                                                                    match random_tweet_action with   
                                                                    | "Simple_Tweet" ->                             
                                                                                                                    total_tweets <- total_tweets + 1
                                                                                                                    let tweet_text = sprintf "%s tweeted -> tweet_%d" curr_id total_tweets
                                                                                                                    ref_server <! ("Tweet",client_id,curr_id,tweet_text,curr_time)
                                                                                                            
                                                                    | "Ment_Hash_Tweet" ->                          
                                                                                                                    total_tweets <- total_tweets+1
                                                                                                                    let mutable user_mentioned = [1 .. num_users].[get_random.Next(num_users)] |> string
                                                                                                                    let hashTag = hashtags.[get_random.Next(hashtags.Length)]
                                                                                                                    // while user_mentioned = curr_id do 
                                                                                                                    //     user_mentioned <- [1 .. num_users].[get_random.Next(num_users)] |> string 
                                                                                                                    let tweet_text = curr_id+"tweeted -> tweet_"+(total_tweets|>string)+" with hashtag #"+hashTag+" and mentioned - @"+user_mentioned
                                                                                                                    printfn "[TWEET] %s" tweet_text
                                                                                                                    ref_server <! ("Tweet",client_id,curr_id,tweet_text,curr_time)
    
                                                                    | "Retweet" ->                                  
                                                                                                                    ref_server <! ("ReTweet",client_id,curr_id,sprintf "user %s doing re-tweet" curr_id,curr_time)
    
                                                                    | "Hash_Tweet" ->                               
                                                                                                                    let hashTag = hashtags.[get_random.Next(hashtags.Length)]
                                                                                                                    total_tweets <- total_tweets+1
                                                                                                                    let tweetMsg = curr_id+"tweeted -> tweet_"+(total_tweets|>string)+" with hashtag #"+hashTag
                                                                                                                    ref_server <! ("Tweet",client_id,curr_id,tweetMsg,curr_time)
    
                                                                    | _ ->                                          ignore()
    
                                                                    tweet_client_system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(duration|>float), mailbox.Self, Tweet)
        | Logoff ->                                             
                                                                user_online <- false

        | Login ->                                              
                                                                user_online <- true
                                                                tweet_client_system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(50.0), mailbox.Self, Tasks)
                                                                tweet_client_system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(100.0), mailbox.Self, Tweet)

        | Tasks ->
                                                                if user_online then
                                                                    let query_actions = [|"Follow"; "Query_Hashtag"; "Query_Mentions"|]
                                                                    let rand_val = Random()
                                                                    let query_action = query_actions.[rand_val.Next(query_actions.Length)]
                                                                    match query_action with
                                                                    | "Follow" ->                                   
                                                                                                                    let mutable random_user = [1 .. num_users].[get_random.Next(num_users)] |> string
                                                                                                                    while random_user = curr_id do 
                                                                                                                        random_user <- [1 .. num_users].[get_random.Next(num_users)] |> string
                                                                                                                    printerRef <! "[FOLLOW] User @"+curr_id+" followed @"+client_id+"_"+ random_user
                                                                                                                    ref_server <! ("Follow",client_id,curr_id,random_user,DateTime.Now)
    
                                                                    | "Query_Hashtag" ->                            
                                                                                                                    let hashTag = hashTagsList.[get_random.Next(hashTagsList.Length)]
                                                                                                                    let mutable ranNumHashtags = [1 .. 5].[get_random.Next(5)]
                                                                                                                    let mutable hashtagStrings = "["
                                                                                                                    for i in 1 .. ranNumHashtags do
                                                                                                                        let twtNum = [1 .. 28].[get_random.Next(28)] |> string
                                                                                                                        hashtagStrings <- hashtagStrings+"tweet"+twtNum
                                                                                                                        if(i <> ranNumHashtags) then
                                                                                                                            hashtagStrings <- hashtagStrings+", "
                                                                                                                    hashtagStrings <- hashtagStrings+"]"
                                                                                                                    printerRef <! "[QUERY_HASHTAG] Hashtag "+hashTag+" mentioned in "+ hashtagStrings
                                                                                                                    ref_server <! ("QueryHashtags",client_id,curr_id,hashTag,DateTime.Now)
    
                                                                    | "Query_Mentions" ->                           
                                                                                                                    let mutable random_user = [1 .. num_users].[get_random.Next(num_users)] |> string
                                                                                                                    let mutable ranNumTweets = [1 .. 5].[get_random.Next(5)]
                                                                                                                    let mutable mentionStrings = "["
                                                                                                                    for i in 1 .. ranNumTweets do
                                                                                                                        let twtNum = [1 .. 28].[get_random.Next(28)] |> string
                                                                                                                        mentionStrings <- mentionStrings+"tweet"+twtNum
                                                                                                                        if(i <> ranNumTweets) then
                                                                                                                            mentionStrings <- mentionStrings+", "
                                                                                                                    mentionStrings <- mentionStrings+"]"
                                                                                                                    printerRef <! "[QUERY_MENTION] User @"+random_user+" mentioned in "+ mentionStrings

                                                                                                                    ref_server <! ("QueryMentions",client_id,curr_id,random_user,DateTime.Now)                                                                                                                        
                                                                    | _ ->                                          ignore()
    
                                                                    tweet_client_system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(100.0), mailbox.Self, Tasks)
            
        return! loop()
    }
    loop()





let client_supervisor (prefix:string) (total_users:int32) (port:string) (mailbox:Actor<_>) = 
    let mutable registered_users = []
    let mutable offline_users = Set.empty
    let mutable user_references = Map.empty
    let mutable users = []
    let mutable user_intervals = Map.empty
    let mutable subscriber_ranks = Map.empty
    let server_ref = tweet_client_system.ActorSelection("akka.tcp://TwitterEngine@"+server_ip+":"+server_port+"/user/ServerActor")


    let rec loop () = actor {
        let! jsonMessage = mailbox.Receive() 
        let message = Json.deserialize<Api_Message> jsonMessage
        let operation = message.operation
        let dataJson = message.data

        match operation with
        | "Start" ->                                            
                                                                let timestamp = DateTime.Now
                                                                let mutable users_list = [| 1 .. total_users |]
            
                                                                shuffle users_list
                                                                users <- users_list |> Array.toList
                                                                for i in [1 .. total_users] do
                                                                    let userkey = users_list.[i-1] |> string
                                                                    subscriber_ranks <- Map.add (prefix+"_"+userkey) ((total_users-1)/i) subscriber_ranks
                                                                    user_intervals <- Map.add (prefix+"_"+userkey) i user_intervals

                                                                server_ref <! ("ClientRegister",prefix,ip_addr,port,timestamp)
                                                                    
        | "User_Online" ->                                        
                                                                let data = Json.deserialize<AckUserReg_Op> dataJson
                                                                let user_id = data.user_id
                                                                user_references.[user_id] <! Login

        | "Signup" ->                                           
                                                                let next_id = (Json.deserialize<Register_Op> dataJson).next_id
                                                                let curr_time = DateTime.Now
                                                                let baseInterval = get_max 5 (total_users/100)
                                                                let mutable curr_id_int = next_id
                                                                let mutable curr_id = prefix+"_"+(users.[curr_id_int-1] |> string) 
                                                                let ref = spawn tweet_client_system curr_id (user_actor curr_id server_ref total_users prefix hashTagsList (baseInterval*user_intervals.[curr_id]))
                                                                user_references <- Map.add curr_id ref user_references
                                                                let subscriber_rank = subscriber_ranks.[curr_id] |> string
                                                                server_ref <! ("UserRegister", prefix, curr_id, subscriber_rank,curr_time)
                                                                registered_users <- curr_id :: registered_users
                                                                
                                                                let message = sprintf "[SIGNUP] User %s registered with server" curr_id
                                                                printerRef <! message         
                                                                user_references.[curr_id] <! Ready

                                                                if curr_id_int < total_users then
                                                                    curr_id_int <- curr_id_int+1
                                                                    let json_data = Json.serialize { next_id = curr_id_int}
                                                                    let op = Json.serialize { operation = "Signup"
                                                                                              data = json_data}
                                                                    tweet_client_system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(50.0), mailbox.Self, op)
                                                                    
        | "Client_Reg_Success" ->                                     
                                                                let json_data = Json.serialize { next_id = 1}
                                                                let op1 = Json.serialize { operation = "Signup"
                                                                                           data = json_data}
                                                                let op2 = Json.serialize { operation = "Offline"
                                                                                           data = "{}"}
                                                                mailbox.Self <! op1
                                                                tweet_client_system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(5.0), mailbox.Self, op2)

        | "Offline" ->                                          
                                                                let rand_val = Random()
                                                                let mutable num_users = registered_users.Length
                                                                num_users <- (30*num_users)/100
                                                                let curr_time = DateTime.Now
                                                                let mutable offline_set = Set.empty
                                                                for i in [1 .. num_users] do
                                                                    let mutable rand_user = registered_users.[rand_val.Next(registered_users.Length)]
                                                                    while offline_users.Contains(rand_user) || offline_set.Contains(rand_user) do
                                                                        rand_user <- registered_users.[rand_val.Next(registered_users.Length)]
                                                                    server_ref <! ("Logout", prefix, rand_user, "", curr_time)
                                                                    user_references.[rand_user] <! Logoff
                                                                    offline_set <- Set.add rand_user offline_set

                                                                for user_id in offline_users do
                                                                    server_ref <! ("Login", prefix, user_id, "",curr_time)
                
                                                                offline_users <- Set.empty
                                                                offline_users <- offline_set

                                                                let operation_json = Json.serialize { operation = "Offline"
                                                                                                      data = "{}"}

                                                                tweet_client_system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(5.0), mailbox.Self, operation_json)

        | _ ->                                                  ignore()

        return! loop()
    }
    loop()

let supervisor = spawn tweet_client_system "ClientSupervisor" (client_supervisor prefix (users |> int32) port)
let operation_json = Json.serialize { operation = "Start"
                                      data = "{}"}
supervisor <! operation_json
tweet_client_system.WhenTerminated.Wait()