#load @"./Constants.fsx"
#load @"./Logger.fsx"
#load @"./MessageTypes.fsx"

#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 
#r "nuget: Akka.Remote" 
#r "nuget: FSharp.Json"


open System
open Akka.FSharp
open MessageTypes
open Constants.Constants
open Logger.Logging
open FSharp.Json

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

let sendMessageToEngine data = 
    let engine = select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system
    let json = Json.serialize data
    engine <! json

let sendMessageToUser data id = 
    let user = select ("akka.tcp://Twitter@127.0.0.1:9001/user/User"+ (id |> string)) system
    let json = Json.serialize data
    user <! json

let UserActor (actorNameVal:string) (actorId:int) (mailbox : Actor<_>) = 

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
            let data = {Author = selfId |> string; Message = ""; Operation = "GoOnline"}
            sendMessageToEngine(data)
            log Debug "Setting %d online" selfId


        let! json = mailbox.Receive()

        let message = Json.deserialize<TweetApiMessage> json
        log Debug "Actor called with %A" message

        mailbox.Sender() |> ignore
        
        let operation = message.Operation

        match operation with

        | "Register" ->
            log Debug "Registered: %s" selfName
            selfStopwatch.Start()

        | "StartUser" ->
            log Debug "Starting All Users"
            if alive then
                let mutable actionId = random.Next(actions.Length)
                let activity = actions.[actionId]
                log Debug "Action selected =======> %s" actions.[actionId]
                match activity with
                | "tweet" -> 
                    let data = {Author = "" |> string; Message = ""; Operation = "TweetInit"}
                    sendMessageToUser data selfId
                | "retweet" ->
                    let data = {Author = "" |> string; Message = ""; Operation = "RetweetInit"}
                    sendMessageToUser data selfId
                | "subscribe" ->
                    let data = {Author = "" |> string; Message = ""; Operation = "SubscribeInit"}
                    sendMessageToUser data selfId
                | "query" ->
                    let data = {Author = "" |> string; Message = ""; Operation = "QueryInit"}
                    sendMessageToUser data selfId
                | _ ->
                    log Debug "Invalid Activity"
            let mutable timeNow = float(selfStopwatch.Elapsed.TotalMilliseconds)
            while float(selfStopwatch.Elapsed.TotalMilliseconds) - timeNow < 10.0 do
                0|> ignore
            let data = {Author = "" |> string; Message = ""; Operation = "StartUser"}
            sendMessageToUser data selfId


        | "GoOffline"  ->
            if alive then
                alive <- false
                oldTime <- float(selfStopwatch.Elapsed.TotalSeconds)

        | "TweetInit" ->
            log Debug "<======== TweetInit =========>"
            let mutable mentionUserBoolean = random.Next(2)
            if mentionUserBoolean = 1 then
                let data = {Author = selfId |> string; Message = ""; Operation = "GetNumNodes"}
                sendMessageToEngine data
            else
                let mutable randomMessageId = random.Next(tweets.Length)
                let mutable randomHashtagId = random.Next(hashtags.Length*2)
                if randomMessageId < tweets.Length then
                    let mutable tweetString = tweets.[randomMessageId]
                    if randomHashtagId < hashtags.Length then
                        tweetString <- tweetString + hashtags.[randomHashtagId]
                    selfTweets <- Array.concat [| selfTweets ; [|tweetString|] |]
                    log Debug "New Tweet from %s is %s" selfName tweetString
                    let data = {Author = selfId |> string; Message = tweetString; Operation = "Tweet"}
                    sendMessageToEngine data
                    allTweetsSent <- allTweetsSent + 1
                    if allTweetsSent >= totalTweetsToBeSent then
                        ALL_COMPUTATIONS_DONE <- 1

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
                log Debug "New Tweet from %s is %s" selfName tweetString
                let data = {Author = selfId |> string; Message = tweetString; Operation = "Tweet"}
                sendMessageToEngine data

        | "ReceiveTweet" ->
            let newTweet = message.Message
            log Debug "Received Tweet %A" newTweet
            receivedTweets <- Array.concat [| receivedTweets ; [|newTweet|] |]

        | "RetweetInit" ->
            log Debug "<======== RetweetInit =========>"
            let data = {Author = selfId |> string; Message = ""; Operation = "Retweet"}
            sendMessageToEngine data

        | "RetweetReceive" ->
            let newTweet = message.Message
            log Debug "Retweet Tweet from %s is %s" selfName newTweet
            let data = {Author = selfId |> string; Message = newTweet; Operation = "Tweet"}
            sendMessageToEngine data

        | "SubscribeInit" ->
            log Debug "<======= SubscribeInit =======>"
            let data = {Author = selfId |> string; Message = ""; Operation = "Subscribe"}
            sendMessageToEngine data

        | "QueryInit" ->
            log Debug "<======= QueryInit =======>"
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
            log Debug "<======= QuerySubscribedTweets =======>"
            let mutable randomSearchId = random.Next(search.Length)
            if randomSearchId < search.Length then
                let mutable randomsearchString = search.[randomSearchId]
                let data = {Author = selfId |> string; Message = randomsearchString; Operation = "QuerySubscribedTweets"}
                sendMessageToEngine data

        | "ReceiveQuerySubscribedTweets" ->
            log Debug "<======= ReceiveQuerySubscribedTweets =======>"
            let searchString = message.Author
            let searchTweetResults = message.Message
            log Debug "QuerySubscribedTweets with search = %s found : %s" searchString searchTweetResults
            

        | "QueryHashtags" ->
            log Debug "<======= QueryHashtags =======>"
            let mutable randomHashtagId = random.Next(hashtags.Length)
            if randomHashtagId < hashtags.Length then
                let data = {Author = selfId |> string; Message = hashtags.[randomHashtagId]; Operation = "QueryHashtags"}
                sendMessageToEngine data

        | "ReceiveQueryHashtags" ->
            log Debug "<======= ReceiveQueryHashtags =======>"
            let searchHashtag = message.Author
            let tweetsFound = message.Message
            log Debug "ReceiveQueryHashtagsTweets with search = %s found : %A" searchHashtag tweetsFound
            

        | "QueryMentions" ->
            log Debug "<======= QueryMentions =======>"
            let data = {Author = selfId |> string; Message = ""; Operation = "QueryMentions"}
            sendMessageToEngine data

        | "ReceiveQueryMentions" ->
            log Debug "<======= ReceiveQueryMentions =======>"
            let tweetsFound = message.Message
            log Debug "ReceiveQueryMentions found : %A" tweetsFound
            

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
        log Debug "<======= MybossActor =======>"
        match message with
        | StartBoss ->
            log Debug "<======= StartBoss =======>"
            for i in 0..numNodes-1 do
                let mutable workerName = sprintf "User%i" i
                let mutable userActor = spawn system workerName (UserActor workerName i)
                let data = {Author = ""; Message = workerName; Operation = "Register"}
                sendMessageToUser data i

            selfStopwatchBoss.Start()
            oldTimeBoss <- float(selfStopwatchBoss.Elapsed.TotalSeconds)

            while float(selfStopwatchBoss.Elapsed.TotalSeconds) - oldTimeBoss < 5.0 do
                0|> ignore

            for i in 0..numNodes-1 do
                let data = {Author = "" |> string; Message = ""; Operation = "StartUser"}
                sendMessageToUser data i

            log Debug "Done with users"

            mailbox.Self <! SimulateBoss
            

        | SimulateBoss ->
            for i in 0..1 do
                let mutable offlineNodeId = random.Next(numNodes)
                log Debug "Setting %i offline" offlineNodeId
                let data = {Author = offlineNodeId |> string; Message = ""; Operation = "GoOffline"}
                sendMessageToEngine data

            while float(selfStopwatchBoss.Elapsed.TotalSeconds) - oldTimeBoss < 1.0 do
                0|> ignore
            oldTimeBoss <- float(selfStopwatchBoss.Elapsed.TotalSeconds)
            mailbox.Self <! SimulateBoss
                
        | StopBoss ->
            ALL_COMPUTATIONS_DONE <- 1

        | _ -> log Debug "Invalid Command"

        return! loop()
    }
    loop ()

//[<EntryPoint>]
let main argv =

    let numNodes = 10
    let numTweets = 1000
    totalTweetsToBeSent <- numTweets
    let bossActor = spawn system "bossActor" (MybossActor numNodes numTweets)
    let destinationRef = select ("akka.tcp://Twitter@127.0.0.1:9001/user/bossActor") system
    destinationRef <! StartBoss

    log Debug "Done with boss"

    while(ALL_COMPUTATIONS_DONE = 0) do
        0 |> ignore

    system.Terminate() |> ignore
    0

main fsi.CommandLineArgs