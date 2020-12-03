#load @"./Constants.fsx"
// #r @"bin/Debug/netcoreapp3.1/Akka.FSharp.dll"
// #r @"bin/Debug/netcoreapp3.1/Akka.dll"
// #r @"bin/Debug/netcoreapp3.1/Akka.Remote.dll"


// #load @"./User.fsx"
#load @"./MessageTypes.fsx"
#load @"./TwitterEngine.fsx"

#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 
#r "nuget: Akka.Remote" 
#r "nuget: FSharp.Json"


open System

// open System.Threading
open Akka.FSharp
// open User.User
open Akka.Actor
open Akka.Configuration
open MessageTypes
open Constants.Constants
open System.Text.RegularExpressions
open FSharp.Json
// open Akka.Configuration
// open Akka.Routing

//<| ConfigurationFactory.Default()

// type TweetMessage(author: string, message: string) =
//     member this.Author = author
//     member this.Message = message
let config =
    Configuration.parse
        @"akka {
            actor.provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            remote.dot-netty.tcp {
                hostname = ""127.0.0.1""
                port = 9001
            }
        }"
        
let mutable allTweetsSent = 0
 
let system = System.create "Twitter" config

let MyUserActor (actorNameVal:string) (actorId:int) (mailbox : Actor<_>) = 

    let selfName = actorNameVal
    let selfId = actorId
    let mutable selfTweets =  Array.create 0 ""
    let mutable receivedTweets = Array.empty
    let selfStopwatch = System.Diagnostics.Stopwatch()
    let mutable oldTime = 0.0
    let mutable alive = true

    let rec loop() = actor {

        if float(selfStopwatch.Elapsed.TotalSeconds) - oldTime > 0.5 && not alive then
            oldTime <- float(selfStopwatch.Elapsed.TotalSeconds)
            alive <- true
            let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
            let data = {Author = selfId |> string; Message = ""; Operation = "GoOnline"}
            let json = Json.serialize data
            destinationRef <! json
            // destinationRef <! GoOnline selfId
            printfn "Setting %d online" selfId


        let! json = mailbox.Receive()

        let message = Json.deserialize<TweetApiMessage> json
        printfn "Actor called with %A" message

        let sender = mailbox.Sender()
        
        let operation = message.Operation

        match operation with

        | "Register" ->
            printfn "Registered: %s" selfName
            selfStopwatch.Start()
            // create Engine and Peers

        | "StartUser" ->
            printfn "Starting All Users"
            if alive then
                let mutable actionId = random.Next(actions.Length)
                printfn "Action selected =======> %s" actions.[actionId]
                if actions.[actionId] = "tweet" then
                    // mailbox.Self <! TweetInit
                    let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (selfId |> string)) system
                    let data = {Author = "" |> string; Message = ""; Operation = "TweetInit"}
                    let json = Json.serialize data
                    destinationRef <! json
                elif actions.[actionId] = "retweet" then
                    // mailbox.Self <! RetweetInit
                    let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (selfId |> string)) system
                    let data = {Author = "" |> string; Message = ""; Operation = "RetweetInit"}
                    let json = Json.serialize data
                    destinationRef <! json
                elif actions.[actionId] = "subscribe" then
                    // mailbox.Self <! SubscribeInit
                    let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (selfId |> string)) system
                    let data = {Author = "" |> string; Message = ""; Operation = "SubscribeInit"}
                    let json = Json.serialize data
                    destinationRef <! json
                elif actions.[actionId] = "query" then
                    // mailbox.Self <! QueryInit
                    let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (selfId |> string)) system
                    let data = {Author = "" |> string; Message = ""; Operation = "QueryInit"}
                    let json = Json.serialize data
                    destinationRef <! json
            let mutable timeNow = float(selfStopwatch.Elapsed.TotalMilliseconds)
            while float(selfStopwatch.Elapsed.TotalMilliseconds) - timeNow < 10.0 do
                0|> ignore
            // mailbox.Self <! "StartUser"
            let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (selfId |> string)) system
            let data = {Author = "" |> string; Message = ""; Operation = "StartUser"}
            let json = Json.serialize data
            destinationRef <! json


        | "GoOffline"  ->
            if alive then
                alive <- false
                oldTime <- float(selfStopwatch.Elapsed.TotalSeconds)

        | "TweetInit" ->
            printfn "<======== TweetInit =========>"
            let mutable mentionUserBoolean = random.Next(2)
            if mentionUserBoolean = 1 then
                let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
                let data = {Author = selfId |> string; Message = ""; Operation = "GetNumNodes"}
                let json = Json.serialize data
                destinationRef <! json
            else
                let mutable randomMessageId = random.Next(tweets.Length)
                let mutable randomHashtagId = random.Next(hashtags.Length*2)
                if randomMessageId < tweets.Length then
                    let mutable tweetString = tweets.[randomMessageId]
                    if randomHashtagId < hashtags.Length then
                        tweetString <- tweetString + hashtags.[randomHashtagId]
                    selfTweets <- Array.concat [| selfTweets ; [|tweetString|] |]
                    printfn "New Tweet from %s is %s" selfName tweetString
                    let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
                    let data = {Author = selfId |> string; Message = tweetString; Operation = "Tweet"}
                    let json = Json.serialize data
                    destinationRef <! json
                    allTweetsSent <- allTweetsSent + 1
                    if allTweetsSent >= totalTweetsToBeSent then
                        ALL_COMPUTATIONS_DONE <- 1
                    // destinationRef <! Tweet(selfId, tweetString)

        | "GetNumNodes" ->
            let totalNodes = message.Message |> int
            let dummyId = message.Author
            let mutable randomMessageId = random.Next(tweets.Length)
            let mutable randomHashtagId = random.Next(hashtags.Length*2)
            if randomMessageId < tweets.Length then
                let mutable tweetString = tweets.[randomMessageId]
                if randomHashtagId < hashtags.Length then
                    tweetString <- tweetString + hashtags.[randomHashtagId]
                let randomUserNameId = random.Next(totalNodes)
                if randomUserNameId < totalNodes then
                    let mutable randomUserName = sprintf "@User%i" randomUserNameId
                    tweetString <- tweetString + randomUserName
                selfTweets <- Array.concat [| selfTweets ; [|tweetString|] |]
                printfn "New Tweet from %s is %s" selfName tweetString
                // let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/TwitterEngineServer") system
                let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
                let data = {Author = selfId |> string; Message = tweetString; Operation = "Tweet"}
                let json = Json.serialize data
                destinationRef <! json
                // destinationRef <! Tweet(selfId, tweetString)

        | "ReceiveTweet" ->
            let newTweet = message.Message
            printfn "Received Tweet %A" newTweet
            receivedTweets <- Array.concat [| receivedTweets ; [|newTweet|] |]

        | "RetweetInit" ->
            printfn "<======== RetweetInit =========>"
            let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
            let data = {Author = selfId |> string; Message = ""; Operation = "Retweet"}
            let json = Json.serialize data
            destinationRef <! json
            // let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/TwitterEngineServer") system
            // destinationRef <! Retweet(selfId)

        | "RetweetReceive" ->
            let newTweet = message.Message
            printfn "Retweet Tweet from %s is %s" selfName newTweet
            // let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/TwitterEngineServer") system
            let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
            let data = {Author = selfId |> string; Message = newTweet; Operation = "Tweet"}
            let json = Json.serialize data
            destinationRef <! json
            // destinationRef <! Tweet(selfId, newTweet)

        | "SubscribeInit" ->
            // let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/TwitterEngineServer") system
            printfn "<======= SubscribeInit =======>"
            let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
            let data = {Author = selfId |> string; Message = ""; Operation = "Subscribe"}
            let json = Json.serialize data
            destinationRef <! json
            // destinationRef <! Subscribe(selfId)

        | "QueryInit" ->
            printfn "<======= QueryInit =======>"
            let mutable randomQueryId = random.Next(queries.Length)
            if randomQueryId < queries.Length then
                if queries.[randomQueryId] = "QuerySubscribedTweets" then
                    let data = {Author = selfId |> string; Message = ""; Operation = "QuerySubscribedTweets"}
                    let json = Json.serialize data
                    mailbox.Self <! json
                elif queries.[randomQueryId] = "QueryHashtags" then
                    let data = {Author = selfId |> string; Message = ""; Operation = "QueryHashtags"}
                    let json = Json.serialize data
                    mailbox.Self <! json
                elif queries.[randomQueryId] = "QueryMentions" then
                    let data = {Author = selfId |> string; Message = ""; Operation = "QueryMentions"}
                    let json = Json.serialize data
                    mailbox.Self <! json
                

        | "QuerySubscribedTweets" ->
            printfn "<======= QuerySubscribedTweets =======>"
            let mutable randomSearchId = random.Next(search.Length)
            if randomSearchId < search.Length then
                let mutable randomsearchString = search.[randomSearchId]
                // let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/TwitterEngineServer") system
                let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
                let data = {Author = selfId |> string; Message = randomsearchString; Operation = "QuerySubscribedTweets"}
                let json = Json.serialize data
                destinationRef <! json
                // destinationRef <! QuerySubscribedTweets(selfId, randomsearchString)

        | "ReceiveQuerySubscribedTweets" ->
            printfn "<======= ReceiveQuerySubscribedTweets =======>"
            let searchString = message.Author
            let searchTweetResults = message.Message
            printfn "QuerySubscribedTweets with search = %s found : %s" searchString searchTweetResults

        | "QueryHashtags" ->
            printfn "<======= QueryHashtags =======>"
            // let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/TwitterEngineServer") system
            let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
            let mutable randomHashtagId = random.Next(hashtags.Length)
            if randomHashtagId < hashtags.Length then
                let data = {Author = selfId |> string; Message = hashtags.[randomHashtagId]; Operation = "QueryHashtags"}
                let json = Json.serialize data
                destinationRef <! json
                // destinationRef <! QueryHashtags(selfId, hashtags.[randomHashtagId])

        | "ReceiveQueryHashtags" ->
            printfn "<======= ReceiveQueryHashtags =======>"
            let searchHashtag = message.Author
            let tweetsFound = message.Message
            printfn "ReceiveQueryHashtagsTweets with search = %s found : %A" searchHashtag tweetsFound

        | "QueryMentions" ->
            printfn "<======= QueryMentions =======>"
            // let dummySelfUserId = message.Author
            // let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/TwitterEngineServer") system
            let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
            let data = {Author = selfId |> string; Message = ""; Operation = "QueryMentions"}
            let json = Json.serialize data
            destinationRef <! json
            // destinationRef <! QueryMentions(selfId)

        | "ReceiveQueryMentions" ->
            printfn "<======= ReceiveQueryMentions =======>"
            let tweetsFound = message.Message
            printfn "ReceiveQueryMentions found : %A" tweetsFound

        | _ -> 0|>ignore


        return! loop()
    }
    loop ()


let MybossActor (numNodesVal:int) (numTweetsVal:int) (mailbox : Actor<_>) = 

    let numNodes = numNodesVal 
    let numTweets = numTweetsVal
    let selfStopwatchBoss = System.Diagnostics.Stopwatch()
    let mutable oldTimeBoss = 0.0
    
    let rec loop() = actor {

        let! message = mailbox.Receive()
        printfn "<======= MybossActor =======>"
        match message with

        | StartBoss ->

            printfn "<======= StartBoss =======>"
            // create Peers
            // let engineActor = spawn system "engineActor" (MyengineActor numNodes numTweets)

            // let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/TwitterEngineServer") system
            // destinationRef <! StartEngine

            // printfn "Done with engine"

            for i in 0..numNodes-1 do
                let mutable workerName = sprintf "User%i" i
                let mutable userActor = spawn system workerName (MyUserActor workerName i)
                let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (i |> string)) system
                let data = {Author = ""; Message = workerName; Operation = "Register"}
                let json = Json.serialize data
                destinationRef <! json

            selfStopwatchBoss.Start()
            oldTimeBoss <- float(selfStopwatchBoss.Elapsed.TotalSeconds)

            while float(selfStopwatchBoss.Elapsed.TotalSeconds) - oldTimeBoss < 5.0 do
                0|> ignore

            // Send signal to all users to start their processes
            for i in 0..numNodes-1 do
                let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (i |> string)) system
                let data = {Author = "" |> string; Message = ""; Operation = "StartUser"}
                let json = Json.serialize data
                destinationRef <! json
                // destinationRef <! StartUser 

            printfn "Done with users"

            mailbox.Self <! SimulateBoss
            

        | SimulateBoss ->

            // choose random num/10 nodes and make them offline
            for i in 0..1 do
                let mutable offlineNodeId = random.Next(numNodes)
                printfn "Setting %i offline" offlineNodeId
                // let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/TwitterEngineServer") system
                let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
                let data = {Author = offlineNodeId |> string; Message = ""; Operation = "GoOffline"}
                let json = Json.serialize data
                destinationRef <! json
                // destinationRef <!  GoOffline(offlineNodeId)
                

            while float(selfStopwatchBoss.Elapsed.TotalSeconds) - oldTimeBoss < 1.0 do
                0|> ignore
            oldTimeBoss <- float(selfStopwatchBoss.Elapsed.TotalSeconds)
            mailbox.Self <! SimulateBoss
                
        | StopBoss ->
            ALL_COMPUTATIONS_DONE <- 1

        | _-> 0|>ignore 

        return! loop()
    }
    loop ()


// main function used to take in parameters
//[<EntryPoint>]
let main argv =
    // let numNodes = ((Array.get argv 1) |> int)
    // let numTweets = ((Array.get argv 2) |> int)

    let numNodes = 10
    let numTweets = 1000
    totalTweetsToBeSent <- numTweets
    let bossActor = spawn system "bossActor" (MybossActor numNodes numTweets)
    let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/bossActor") system
    destinationRef <! StartBoss

    printfn "Done with boss"

    while(ALL_COMPUTATIONS_DONE = 0) do
        0|>ignore

    system.Terminate() |> ignore
    0

main fsi.CommandLineArgs