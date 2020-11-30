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
open MessageTypes
open Constants.Constants
open System.Text.RegularExpressions
open TwitterEngine.TwitterEngine
// open Akka.Configuration
// open Akka.Routing

//<| ConfigurationFactory.Default()

// type TweetMessage(author: string, message: string) =
//     member this.Author = author
//     member this.Message = message
    

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
            let destinationRef = select ("akka://system/user/engineActor") system
            destinationRef <! GoOnline selfId
            printfn "Setting %d online" selfId


        let! message = mailbox.Receive()
        match message with

        | Register ->
            selfStopwatch.Start()
            // create Engine and Peers

        | StartUser ->
            if alive then
                let mutable actionId = random.Next(actions.Length)
                if actions.[actionId] = "tweet" then
                    mailbox.Self <! TweetInit
                elif actions.[actionId] = "retweet" then
                    mailbox.Self <! RetweetInit
                elif actions.[actionId] = "subscribe" then
                    mailbox.Self <! SubscribeInit
                elif actions.[actionId] = "query" then
                    mailbox.Self <! QueryInit
            let mutable timeNow = float(selfStopwatch.Elapsed.TotalMilliseconds)
            while float(selfStopwatch.Elapsed.TotalMilliseconds) - timeNow < 10.0 do
                0|> ignore
            mailbox.Self <! StartUser


        | GoOffline(myId) ->
            if alive then
                alive <- false
                oldTime <- float(selfStopwatch.Elapsed.TotalSeconds)

        | TweetInit ->
            let mutable mentionUserBoolean = random.Next(2)
            if mentionUserBoolean = 1 then
                let destinationRef = select ("akka://system/user/engineActor") system
                destinationRef <! GetNumNodes(0, selfId)
            else
                let mutable randomMessageId = random.Next(tweets.Length)
                let mutable randomHashtagId = random.Next(hashtags.Length*2)
                if randomMessageId < tweets.Length then
                    let mutable tweetString = tweets.[randomMessageId]
                    if randomHashtagId < hashtags.Length then
                        tweetString <- tweetString + hashtags.[randomHashtagId]
                    selfTweets <- Array.concat [| selfTweets ; [|tweetString|] |]
                    printfn "New Tweet from %s is %s" selfName tweetString
                    let destinationRef = select ("akka://system/user/engineActor") system
                    destinationRef <! Tweet(selfId, tweetString)

        | GetNumNodes(totalNodes, dummyId) ->
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
                let destinationRef = select ("akka://system/user/engineActor") system
                destinationRef <! Tweet(selfId, tweetString)

        | ReceiveTweet(newTweet) ->
            printfn "Received Tweet %A" newTweet
            receivedTweets <- Array.concat [| receivedTweets ; [|newTweet|] |]

        | RetweetInit ->
            let destinationRef = select ("akka://system/user/engineActor") system
            destinationRef <! Retweet(selfId)

        | RetweetReceive(newTweet) ->
            printfn "Retweet Tweet from %s is %s" selfName newTweet
            let destinationRef = select ("akka://system/user/engineActor") system
            destinationRef <! Tweet(selfId, newTweet)

        | SubscribeInit ->
            let destinationRef = select ("akka://system/user/engineActor") system
            destinationRef <! Subscribe(selfId)

        | QueryInit ->
            let mutable randomQueryId = random.Next(queries.Length)
            if randomQueryId < queries.Length then
                if queries.[randomQueryId] = "QuerySubscribedTweets" then
                    mailbox.Self <! QuerySubscribedTweets
                elif queries.[randomQueryId] = "QueryHashtags" then
                    mailbox.Self <! QueryHashtags(0, "")
                elif queries.[randomQueryId] = "QueryMentions" then
                    mailbox.Self <! QueryMentions

        | QuerySubscribedTweets(dummyId, dummyStr) ->
            let mutable randomSearchId = random.Next(search.Length)
            if randomSearchId < search.Length then
                let mutable randomsearchString = search.[randomSearchId]
                let destinationRef = select ("akka://system/user/engineActor") system
                destinationRef <! QuerySubscribedTweets(selfId, randomsearchString)

        | ReceiveQuerySubscribedTweets(searchString, searchTweetResults) ->
            printfn "QuerySubscribedTweets with search = %s found : %A" searchString searchTweetResults

        | QueryHashtags(someDummyNumber, someDummyString) ->
            let destinationRef = select ("akka://system/user/engineActor") system
            let mutable randomHashtagId = random.Next(hashtags.Length)
            if randomHashtagId < hashtags.Length then
                destinationRef <! QueryHashtags(selfId, hashtags.[randomHashtagId])

        | ReceiveQueryHashtags(searchHashtag, tweetsFound) ->
            printfn "ReceiveQueryHashtagsTweets with search = %s found : %A" searchHashtag tweetsFound

        | QueryMentions(dummySelfUserId) ->
            let destinationRef = select ("akka://system/user/engineActor") system
            destinationRef <! QueryMentions(selfId)

        | ReceiveQueryMentions(tweetsFound) ->
            printfn "ReceiveQueryMentions found : %A" tweetsFound

        |_ -> 0|>ignore


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
        match message with

        | StartBoss ->

            // create Peers
            let engineActor = spawn system "engineActor" (MyengineActor numNodes numTweets)

            let destinationRef = select ("akka://system/user/engineActor") system
            destinationRef <! StartEngine

            printfn "Done with engine"

            for i in 0..numNodes-1 do
                let mutable workerName = sprintf "User%i" i
                let mutable userActor = spawn system workerName (MyUserActor workerName i)
                userActor <! Register

            selfStopwatchBoss.Start()
            oldTimeBoss <- float(selfStopwatchBoss.Elapsed.TotalSeconds)

            while float(selfStopwatchBoss.Elapsed.TotalSeconds) - oldTimeBoss < 5.0 do
                0|> ignore

            // Send signal to all users to start their processes
            for i in 0..numNodes-1 do
                let destinationRef = select ("akka://system/user/User"+ (i |> string)) system
                destinationRef <! StartUser 

            printfn "Done with users"

            mailbox.Self <! SimulateBoss
            

        | SimulateBoss ->

            // choose random num/10 nodes and make them offline
            for i in 0..1 do
                let mutable offlineNodeId = random.Next(numNodes)
                printfn "Setting %i offline" offlineNodeId
                let destinationRef = select ("akka://system/user/engineActor") system
                destinationRef <!  GoOffline(offlineNodeId)
                

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
    let numNodes = ((Array.get argv 1) |> int)
    let numTweets = ((Array.get argv 2) |> int)

    let bossActor = spawn system "bossActor" (MybossActor numNodes numTweets)
    let destinationRef = select ("akka://system/user/bossActor") system
    destinationRef <! StartBoss

    printfn "Done with boss"

    while(ALL_COMPUTATIONS_DONE = 0) do
        0|>ignore

    system.Terminate() |> ignore
    0

main fsi.CommandLineArgs