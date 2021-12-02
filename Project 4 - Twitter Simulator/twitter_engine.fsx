#time "on"
#r "nuget: Akka"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#r "nuget: FSharp.Json"

open System
open Akka.Actor
open Akka.FSharp
open Akka.Remote
open System.IO;
open FSharp.Json

//Method to fetch machine ip address
let find_machine_ip = 
    let list = System.Net.Dns.GetHostEntry(System.Net.Dns.GetHostName()).AddressList
    let mutable ip_addr = "localhost"
    for i = list.Length - 1 downto 0 do
        if (list.[i].AddressFamily.ToString() = "InterNetwork") then
            ip_addr <- list.[i].ToString()
    ip_addr

//Find Maximum
let get_max x y = 
    if x > y then x
    else y

//Find Minimum
let get_min x y = 
    if x < y then x
    else y

let server_ip = find_machine_ip

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
                port = 3536
                hostname = "+server_ip+"
            }
        }")


let tweet_server_system = System.create "TwitterEngine" configuration
let perf_log = "Perflog.log"

type Api_Message = {
    operation: string
    data: string
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

type HashTagMessages = 
    | Extract_Hashtags of (string*string*string)
    | InitPrinter_HT of (Map<string,ActorSelection>)
    | Query_Hashtags of (string*string*string*DateTime)

type UserMessages = 
    | Initialize of (IActorRef*IActorRef*IActorRef)
    | InitPrinter_Users of (Map<string,ActorSelection>)
    | Get_Stats of (Map<string,string>*uint64*DateTime)
    | Register of (string*string*string*DateTime)
    | Offline of (string*string*DateTime)
    | Online of (string*string*IActorRef*DateTime)
    | Refresh_Feed of (string*string*string*string*DateTime)
    | Follow of (string*string*string*DateTime)
    
type TweetMessages = 
    | InitTweet of (IActorRef*IActorRef)
    | InitPrinter of (Map<string,ActorSelection>)
    | Tweet of (string*string*string*DateTime*IActorRef)
    | Update_Stats of (Map<string,Set<string>>*Map<string,string>*uint64)
    | Increment_Tweets of (string)

type ShowFeedMessages = 
    | ShowFeeds of (string*string*IActorRef)
    | UpdateFeedTable of (string*string*string)
    | InitPrinter_SF of (Map<string,ActorSelection>)
    
type RetweetMessages = 
    | Init_RT of (IActorRef*IActorRef)
    | Retweet of (string*string*DateTime)
    | Update_Feed_Table of (string*string*string)
    | InitPrinter_RT of (Map<string,ActorSelection>)

type MentionsMessages = 
    | InitMentions of (IActorRef)
    | Store_Mentions of (string*string)
    | Extract_Mentions of (string*string*string*DateTime)
    | InitPrinter_Mentions of (Map<string,ActorSelection>)
    | QueryMentions of (string*string*string*DateTime)


let TweetActor (mailbox:Actor<_>) = 
    let mutable usersActor = Unchecked.defaultof<IActorRef>
    let mutable hashTagActor = Unchecked.defaultof<IActorRef>
    let mutable tweet_output = Map.empty
    let mutable total_tweets = 0.0
    let mutable tweet_count_map = Map.empty
    let mutable server_time = 0.0
    let rec loop () = actor {
        let! message = mailbox.Receive() 
        let curr_time = DateTime.Now

        match message with
        | InitTweet(hashtag_ref,user_ref) ->                      
                                                            hashTagActor <- hashtag_ref
                                                            usersActor <- user_ref

        | Tweet(cli_id, user_id, tweet_text, 
                    action_time,supervisor) ->         
                                                            total_tweets <- total_tweets+1.0
                                                            let mutable tweet_count = 0
                                                            tweet_output.[cli_id] <! sprintf "[TWEET] %s" tweet_text

                                                            if tweet_count_map.ContainsKey user_id then 
                                                                tweet_count <- tweet_count_map.[user_id] + 1
                                                                tweet_count_map <- Map.remove user_id tweet_count_map
                                                            tweet_count_map <- Map.add user_id tweet_count tweet_count_map

                                                            hashTagActor <! Extract_Hashtags(cli_id,user_id,tweet_text)
                                                            usersActor <! Refresh_Feed(cli_id,user_id,tweet_text, "tweeted", DateTime.Now)
                                                            server_time <- server_time + (curr_time.Subtract action_time).TotalMilliseconds
                                                            let mean_time = server_time / total_tweets
                                                            supervisor <! ("ServiceStats","","Tweet",(mean_time |> string),DateTime.Now)
                                                            
        | Update_Stats(follow_map,request_stats,
                                            req_ps) ->      
                                                            File.WriteAllText(perf_log, "")
                                                            File.AppendAllText(perf_log, ("\n"+curr_time.ToString()))
                                                            File.AppendAllText(perf_log, (sprintf "\nNumber of user requests handled per second = %u\n" req_ps))
                                                            File.AppendAllText(perf_log, "\nAverage time taken for service(s) in ms:")
                                                            for i in request_stats do
                                                                File.AppendAllText(perf_log, (sprintf "\n%s = %s" i.Key i.Value))

                                                            File.AppendAllText(perf_log, "\n\nUserID\t\tTotal Tweets\n")
                                                            for user_id in follow_map do
                                                                if tweet_count_map.ContainsKey user_id.Key then
                                                                    let stat = sprintf "%s\t%s\t%s\n" user_id.Key (user_id.Value.Count |> string) (tweet_count_map.[user_id.Key] |> string)
                                                                    File.AppendAllText(perf_log, stat)

        | Increment_Tweets(user_id) ->                              
                                                            if tweet_count_map.ContainsKey user_id then 
                                                                let tweet_count = (tweet_count_map.[user_id] + 1)
                                                                tweet_count_map <- Map.remove user_id tweet_count_map  
                                                                tweet_count_map <- Map.add user_id tweet_count tweet_count_map
                                                                    
        | InitPrinter(logger) ->                 
                                                            tweet_output <- logger

        return! loop()
    }
    loop()
    
let ShowfeedActor (mailbox:Actor<_>) = 
    let mutable user_feed_map = Map.empty
    let mutable logger_map = Map.empty

    let rec loop () = actor {
        let! message = mailbox.Receive() 
        let curr_time = DateTime.Now

        match message with
        | UpdateFeedTable(user_id, _, tweet_text) ->                   
                                                                let mutable feed_list = []
                                                                if user_feed_map.ContainsKey user_id then
                                                                    feed_list <- user_feed_map.[user_id]
                                                                feed_list  <- tweet_text :: feed_list
                                                                user_feed_map <- Map.remove user_id user_feed_map
                                                                user_feed_map <- Map.add user_id feed_list user_feed_map

        | ShowFeeds(cli_id, user_id, 
                        cli_supervisor) ->                    
                                                                if user_feed_map.ContainsKey user_id then
                                                                    let mutable feedsTop = ""
                                                                    let feedList:List<string> = user_feed_map.[user_id]
                                                                    let feed_size = get_min 10 feedList.Length
                                                                    for i in [0..(feed_size-1)] do
                                                                        feedsTop <- "\n" + user_feed_map.[user_id].[i]

                                                                    logger_map.[cli_id] <! sprintf "[ONLINE] User %s is online..Feeds -> %s"  user_id feedsTop
                                                                else
                                                                    logger_map.[cli_id] <! sprintf "[ONLINE] User %s is online..No feeds yet!!!"  user_id
                                                                cli_supervisor <! ("User_Online", user_id, "", "", "")

        | InitPrinter_SF(ob) ->                   
                                                                logger_map <- ob

        return! loop()
    }
    loop()

let RetweetActor (mailbox:Actor<_>) = 

    let mutable user_actor = Unchecked.defaultof<IActorRef>
    let mutable tweets_actor = Unchecked.defaultof<IActorRef>

    let mutable user_feed_map = Map.empty
    let mutable logger_map = Map.empty
    let mutable rand_val = Random()
    let mutable num_retweets = 0.0
    let mutable rt_time = 0.0

    let rec loop () = actor {
        let! message = mailbox.Receive() 
        let curr_time = DateTime.Now

        match message with
        | Init_RT(user_ref,tweet_ref) ->                         
                                                                user_actor <- user_ref
                                                                tweets_actor <- tweet_ref

        | Update_Feed_Table(user_id,_,tweet_text) ->                        
                                                                let mutable tweet_list = []
                                                                if user_feed_map.ContainsKey user_id then
                                                                    tweet_list <- user_feed_map.[user_id]
                                                                tweet_list  <- tweet_text :: tweet_list
                                                                user_feed_map <- Map.remove user_id user_feed_map
                                                                user_feed_map <- Map.add user_id tweet_list user_feed_map

        | Retweet(cli_id,user_id,reqTime) ->                           
                                                                if user_feed_map.ContainsKey user_id then   
                                                                    num_retweets <- num_retweets + 1.0
                                                                    let random_tweet = user_feed_map.[user_id].[rand_val.Next(user_feed_map.[user_id].Length)]
                                                                    logger_map.[cli_id] <! sprintf "[RE_TWEET] %s retweeted -> %s"  user_id random_tweet
                                                                    rt_time <- rt_time + (curr_time.Subtract reqTime).TotalMilliseconds
                                                                    let averageTime = rt_time / num_retweets
                                                                    mailbox.Sender() <! ("ServiceStats","","ReTweet",(averageTime |> string),DateTime.Now)
                                                                    user_actor <! Refresh_Feed(cli_id,user_id,random_tweet,"retweeted",DateTime.Now)
                                                                    tweets_actor <! Increment_Tweets(user_id)
                                                                    
        | InitPrinter_RT(logger) ->                    
                                                                logger_map <- logger

        return! loop()
    }
    loop()

let HashTagsActor (mailbox:Actor<_>) = 
    
    let mutable logger_map = Map.empty
    let mutable hashtag_map = Map.empty
    
    let mutable query_count = 1.0
    let mutable query_time = 1.0

    let rec loop () = actor {
        let! message = mailbox.Receive() 
        let curr_time = DateTime.Now

        match message with
        | InitPrinter_HT(logger) ->                   
                                                                logger_map <- logger
                                                                
        | Extract_Hashtags(_,_,tweet_text) ->                             
                                                                let parsed_tweet = tweet_text.Split ' '
                                                                for i in parsed_tweet do
                                                                if i.[0] = '#' then
                                                                    let parsedTag = i.[1..(i.Length-1)]
                                                                    if not (hashtag_map.ContainsKey parsedTag) then
                                                                        hashtag_map <- Map.add parsedTag List.empty hashtag_map
                                                                    let mutable tweet_list = hashtag_map.[parsedTag]
                                                                    tweet_list <- tweet_text :: tweet_list
                                                                    hashtag_map <- Map.remove parsedTag hashtag_map
                                                                    hashtag_map <- Map.add parsedTag tweet_list hashtag_map
                                                                    ignore()

        | Query_Hashtags(cli_id,user_id,hashtag,
                                    operation_time) ->             
                                                                if logger_map.ContainsKey cli_id then
                                                                    query_count <- query_count+1.0
                                                                    if hashtag_map.ContainsKey hashtag then
                                                                        let mutable hash_length = get_min 10 hashtag_map.[hashtag].Length
                                                                        let mutable tagsstring = ""
                                                                        for i in [0..(hash_length-1)] do
                                                                            tagsstring <- "\n" + hashtag_map.[hashtag].[i]
                                                                        logger_map.[cli_id] <! sprintf "[QUERY_HASHTAG] by user %s: Recent 10(Max) tweets for hashTag #%s ->%s"  user_id hashtag tagsstring
                                                                    else    
                                                                        logger_map.[cli_id] <! sprintf "[QUERY_HASHTAG] by user %s: No tweets for hashTag #%s"  user_id hashtag
                
                                                                    query_time <- query_time + (curr_time.Subtract operation_time).TotalMilliseconds

                                                                    let mean_time = (query_time / query_count)
                                                                    mailbox.Sender() <! ("ServiceStats","","QueryHashTag",(mean_time |> string),DateTime.Now)

        return! loop()
    }
    loop()

let MentionsActor (mailbox:Actor<_>) = 

    let mutable tweet_actor = Unchecked.defaultof<IActorRef>

    let mutable Mention_map = Map.empty
    let mutable logger_map = Map.empty

    let mutable query_time = 1.0
    let mutable query_count = 1.0
    let mutable users = Set.empty

    let rec loop () = actor {
        let! message = mailbox.Receive() 
        let curr_time = DateTime.Now

        match message with
        | InitMentions(tweet_ref) ->                               
                                                                tweet_actor <- tweet_ref

        | Store_Mentions(_,user_id) ->                        
                                                                users <- Set.add user_id users
                                                                Mention_map <- Map.add user_id List.empty Mention_map

        | Extract_Mentions(cli_id, user_id, tweet_text,
                                        action_time) ->           
                                                                // if users.Contains user_id then
                                                                    let parsed_tweet = tweet_text.Split ' '
                                                                    let mutable mentions = false
                                                                    for i in parsed_tweet do
                                                                        if i.[0] = '@' then
                                                                            mentions <- true
                                                                            let parsedMention = i.[1..(i.Length-1)]
                                                                            if users.Contains parsedMention then
                                                                                let mutable mList = Mention_map.[parsedMention]
                                                                                mList <- tweet_text :: mList
                                                                                Mention_map <- Map.remove parsedMention Mention_map
                                                                                Mention_map <- Map.add parsedMention mList Mention_map
                                                                                tweet_actor <! Tweet(cli_id, user_id, tweet_text,action_time,mailbox.Sender())
                                                                    if not mentions then 
                                                                        tweet_actor <! Tweet(cli_id, user_id, tweet_text,action_time,mailbox.Sender())

        | QueryMentions(cli_id,user_id,mention,
                                    action_time) ->         
                                                                if logger_map.ContainsKey cli_id then
                                                                    query_count <- query_count+1.0

                                                                    if Mention_map.ContainsKey mention then
                                                                        let mutable mSize = 10
                                                                        if (Mention_map.[mention].Length < 10) then
                                                                            mSize <- Mention_map.[mention].Length
                                                                        let mutable tweetsstring = ""
                                                                        for i in [0..(mSize-1)] do
                                                                            tweetsstring <- "\n" + Mention_map.[mention].[i]
                                                                        logger_map.[cli_id] <! sprintf " by user %s: Recent 10(Max) tweets for user @%s ->%s" user_id mention tweetsstring
                                                                    else
                                                                        logger_map.[cli_id] <! sprintf " by user %s: No tweets for user @%s"  user_id mention
                                                                    query_time <- query_time + (curr_time.Subtract action_time).TotalMilliseconds
                                                                    let averageTime = query_time / query_count

                                                                    mailbox.Sender() <! ("ServiceStats","","QueryMentions",(averageTime |> string),DateTime.Now)
                                                                    
        | InitPrinter_Mentions(logger) ->                   
                                                                logger_map <- logger

        return! loop()
    }
    loop()

let UsersActor (mailbox:Actor<_>) = 

    let mutable retweet_actor = mailbox.Self
    let mutable showfeed_actor = mailbox.Self
    let mutable tweet_actor = mailbox.Self

    let mutable logger_map = Map.empty
    let mutable user_set = Set.empty
    let mutable follow_map = Map.empty
    let mutable offline_users = Set.empty
    let mutable subscriber_rank = Map.empty

    let mutable count = 1.0
    let mutable follow_duration = 1.0

    let rec loop () = actor {
        let! message = mailbox.Receive()  
        let curr_time = DateTime.Now

        match message with
        | Initialize(retweet_ref, showfeed_ref, tweet_ref) ->             
                                                                retweet_actor <- retweet_ref
                                                                showfeed_actor <- showfeed_ref
                                                                tweet_actor <- tweet_ref

        | Register(_, user_id, sub_count,
                                action_time) ->               
                                                                user_set <- Set.add user_id user_set
                                                                subscriber_rank <- Map.add user_id (sub_count |> int) subscriber_rank
                                                                follow_map <- Map.add user_id Set.empty follow_map
                                                                follow_duration <- follow_duration + (curr_time.Subtract action_time).TotalMilliseconds
                                                                count <- count + 1.0

        | Follow(cli_id, user_id, follow_id, reqTime) ->                 
                                                                count <- count + 1.0
                                                                if follow_map.ContainsKey follow_id && not (follow_map.[follow_id].Contains user_id) && follow_map.[follow_id].Count < subscriber_rank.[follow_id] then
                                                                    let mutable st = follow_map.[follow_id]
                                                                    st <- Set.add user_id st
                                                                    follow_map <- Map.remove follow_id follow_map
                                                                    follow_map <- Map.add follow_id st follow_map
                                                                    logger_map.[cli_id] <! sprintf "[FOLLOW] User %s started following %s"  user_id follow_id
                                                                follow_duration <- follow_duration + (curr_time.Subtract reqTime).TotalMilliseconds
                                                                
        | Get_Stats(stats, perf, reqTime) ->                    
                                                                count <- count + 1.0
                                                                tweet_actor <! Update_Stats(follow_map,stats,perf)
                                                                follow_duration <- follow_duration + (curr_time.Subtract reqTime).TotalMilliseconds

        | Refresh_Feed(_,user_id,tweet_text,message,
                                        action_time) ->       
                                                                count <- count + 1.0
                                                                for i in follow_map.[user_id] do
                                                                    showfeed_actor <! UpdateFeedTable(i, user_id, tweet_text) 
                                                                    retweet_actor <! Update_Feed_Table(i, user_id, tweet_text)
                                                                    if not (offline_users.Contains i) then
                                                                        let splits = i.Split '_'
                                                                        let id = splits.[0]
                                                                        if message = "tweeted" then
                                                                            logger_map.[id] <! sprintf "[NEW_FEED] For User: %s -> %s"  i tweet_text    
                                                                        else
                                                                            logger_map.[id] <! sprintf "[NEW_FEED] For User: %s -> %s %s - %s"  i user_id message tweet_text
                                                                follow_duration <- follow_duration + (curr_time.Subtract action_time).TotalMilliseconds   
                                                                
        | Online(cli_id, user_id, client_supervisor, 
                                        action_time) ->              
                                                                offline_users <- Set.remove user_id offline_users
                                                                showfeed_actor <! ShowFeeds(cli_id, user_id, client_supervisor)
                                                                follow_duration <- follow_duration + (curr_time.Subtract action_time).TotalMilliseconds
                                                                count <- count + 1.0
                                                                
        | Offline(cli_id, user_id, action_time) ->                     
                                                                offline_users <- Set.add user_id offline_users
                                                                follow_duration <- follow_duration + (curr_time.Subtract action_time).TotalMilliseconds
                                                                count <- count + 1.0
                                                                logger_map.[cli_id] <! sprintf "[OFFLINE] User %s is going offline"  user_id
                                                                
        | InitPrinter_Users(logger) ->                              
                                                                logger_map <- logger

        let avg_time = follow_duration / count
        mailbox.Sender() <! ("ServiceStats","","Follow/Offline/Online",(avg_time |> string),DateTime.Now)
        
        return! loop()
    }
    loop()

let ServerActor (mailbox:Actor<_>) = 

    let mutable req_count = 0UL
    let mutable start_time = DateTime.Now
    let mutable stats_map = Map.empty
    let mutable logger_map = Map.empty
    
    let mutable hashtagactor = Unchecked.defaultof<IActorRef>
    let mutable tweetactor = Unchecked.defaultof<IActorRef>
    let mutable mentionsactor = Unchecked.defaultof<IActorRef>
    let mutable usersactor = Unchecked.defaultof<IActorRef>
    let mutable retweetactor = Unchecked.defaultof<IActorRef>
    let mutable showfeedactor = Unchecked.defaultof<IActorRef>

    let update_loggers logger = 
        retweetactor <! InitPrinter_RT(logger)
        tweetactor <! InitPrinter(logger)
        showfeedactor <! InitPrinter_SF(logger)
        hashtagactor <! InitPrinter_HT(logger)
        mentionsactor <! InitPrinter_Mentions(logger)
        usersactor <! InitPrinter_Users(logger)

    let rec loop () = actor {
        let! (message:obj) = mailbox.Receive()
        let (operation,_,_,_,_) : Tuple<string,string,string,string,DateTime> = downcast message 

        let curr_time = DateTime.Now

        match operation with
        | "Start" ->                                            
                                                                usersactor <- spawn tweet_server_system (sprintf "UsersActor") UsersActor 
                                                                hashtagactor <- spawn tweet_server_system (sprintf "HashTagsActor") HashTagsActor
                                                                retweetactor <- spawn tweet_server_system (sprintf "RetweetActor") RetweetActor
                                                                tweetactor <- spawn tweet_server_system (sprintf "TweetActor") TweetActor
                                                                showfeedactor <- spawn tweet_server_system (sprintf "ShowfeedActor") ShowfeedActor
                                                                mentionsactor <- spawn tweet_server_system (sprintf "MentionsActor") MentionsActor

                                                                usersactor <! Initialize(retweetactor, showfeedactor, tweetactor)
                                                                mentionsactor <! InitMentions(tweetactor)
                                                                tweetactor <! InitTweet(hashtagactor,usersactor)
            
                                                                retweetactor <! Init_RT(usersactor,tweetactor)
                                                                start_time <- DateTime.Now
                                                                tweet_server_system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(5000.0), mailbox.Self, ("PrintStats","","","",DateTime.Now))

        | "ClientRegister" ->                                   
                                                                let (_,cli_id,cli_ip,port,_) : Tuple<string,string,string,string,DateTime> = downcast message 
                                                                req_count <- req_count + 1UL
                                                                let logger_ref = tweet_server_system.ActorSelection(sprintf "akka.tcp://TwitterSimulator@%s:%s/user/Logger" cli_ip port)
                                                                logger_map <- Map.add cli_id logger_ref logger_map
                                                                update_loggers logger_map
                                                                let op = Json.serialize { operation = "Client_Reg_Success"
                                                                                          data = "{}"}
                                                                mailbox.Sender() <! op

        | "UserRegister" ->                                     
                                                                let (_,cli_id,user_id,subscribers,action_time) : Tuple<string,string,string,string,DateTime> = downcast message 
                                                                req_count <- req_count + 1UL
                                                                usersactor <! Register(cli_id, user_id, subscribers,action_time)
                                                                mentionsactor <! Store_Mentions(cli_id, user_id)

        | "Login" ->                                            
                                                                let (_,cli_id,userid,_,reqTime) : Tuple<string,string,string,string,DateTime> = downcast message 
                                                                req_count <- req_count + 1UL
                                                                usersactor <! Online(cli_id, userid, mailbox.Sender(),reqTime)

        | "Logout" ->                                           
                                                                let (_,cli_id,userid,_,reqTime) : Tuple<string,string,string,string,DateTime> = downcast message 
                                                                req_count <- req_count + 1UL
                                                                usersactor <! Offline(cli_id, userid,reqTime)

        | "Follow" ->                                           
                                                                let (_,cli_id,userid,followingid,reqTime) : Tuple<string,string,string,string,DateTime> = downcast message 
                                                                req_count <- req_count + 1UL
                                                                usersactor <! Follow(cli_id, userid, followingid, reqTime)

        | "Tweet" ->                                            
                                                                let (_,cli_id,userid,twt,reqTime) : Tuple<string,string,string,string,DateTime> = downcast message 
                                                                req_count <- req_count + 1UL                                                                
                                                                mentionsactor <! Extract_Mentions(cli_id,userid,twt,reqTime)

        | "ReTweet" ->                                          
                                                                let (_,cli_id,userid,_,reqTime) : Tuple<string,string,string,string,DateTime> = downcast message   
                                                                req_count <- req_count + 1UL
                                                                retweetactor <! Retweet(cli_id,userid,reqTime)

        | "QueryMentions" ->                                    
                                                                let (_,cli_id,user_id,mention,reqTime) : Tuple<string,string,string,string,DateTime> = downcast message  
                                                                req_count <- req_count + 1UL
                                                                mentionsactor <! QueryMentions(cli_id,user_id,mention,reqTime)

        | "Query_Hashtags" ->                                    
                                                                let (_,cli_id,user_id,tag,reqTime) : Tuple<string,string,string,string,DateTime> = downcast message  
                                                                req_count <- req_count + 1UL
                                                                hashtagactor <! Query_Hashtags(cli_id,user_id,tag,reqTime)

        | "ServiceStats" ->                                     
                                                                let (_,_,key,value,_) : Tuple<string,string,string,string,DateTime> = downcast message 
                                                                if key <> "" then
                                                                    if stats_map.ContainsKey key then
                                                                        stats_map <- Map.remove key stats_map
                                                                    stats_map <- Map.add key value stats_map

        | "PrintStats" ->                                       
                                                                let mutable perf = 0UL
                                                                let timediff = (DateTime.Now-start_time).TotalSeconds |> uint64
                                                                if req_count > 0UL then
                                                                    perf <- req_count/timediff
                                                                    usersactor <! Get_Stats(stats_map, perf, DateTime.Now)  
                                                                    printfn "Server uptime = %u seconds, requests served = %u, Avg requests served = %u per second" timediff req_count perf
                                                                tweet_server_system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(5000.0), mailbox.Self, ("PrintStats","","","",DateTime.Now))

        | _ ->                                                  ignore()

        return! loop()
    }
    loop()

let supervisor = spawn tweet_server_system "ServerActor" ServerActor
supervisor <! ("Start","","","",DateTime.Now)
tweet_server_system.WhenTerminated.Wait()