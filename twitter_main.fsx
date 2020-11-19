#r @"bin/Debug/netcoreapp3.1/Akka.FSharp.dll"
#r @"bin/Debug/netcoreapp3.1/Akka.dll"
#r @"bin/Debug/netcoreapp3.1/Akka.Remote.dll"

open System
open System.Threading
open Akka.FSharp
open Akka.Configuration
open Akka.Routing
open System.Diagnostics
open System.Text.RegularExpressions

let mutable ALL_COMPUTATIONS_DONE = 0
let mutable bossActorRef = Array.empty
let mutable engineActorRef = Array.empty
let mutable UserActorRef = Array.empty
let mutable UserActorMapping = Map.empty

// Define the type of messages this program can send or receive
type MyMessage =
| Join of int*int
| StartBoss
| SimulateBoss
| StopBoss
| StartEngine
| Register
| StartUser
| TweetInit
| Tweet of int*string
| ReceiveTweet of string
| RetweetInit
| Retweet of string
| RetweetReceive of string
| SendRandomTweet of string*string
| Subscribe of int
| SubscribeInit
| QueryInit
| QuerySubscribedTweets
| QueryHashtags of int*string
| ReceiveQueryHashtags of string[]
| QueryMentions
| ReceiveQueryMentions of string[]
| DeliverTweet of string[]
| GoOffline of int
| GoOnline of int
| GetNumNodes


let actions = [|"tweet"; "tweet"; "subscribe"; "retweet"; "query"|]
let queries = [|"QuerySubscribedTweets"; "QueryHashtags"; "QueryMentions"|]
let hashtags = [|"#COP5615isgreat"; "#FSharp"; "#Pikachu"; "#Pokemon"; "#GoGators"; "#MarstonLibrary"|]
let search = [|"DOS"; "Coding"; "Pokemon"; "UF"; "Guess"|]
let tweets = [|"Doing DOS rn, talk later!"; "Coding takes time!"; "Watching Pokemon!"; "Playing Pokemon Go!" ; "UF is awesome!"; "Guess what?"|]
let random = System.Random()
let system = System.create "system" <| ConfigurationFactory.Default()


let MyUserActor (actorNameVal:string) (actorId:int) (mailbox : Actor<_>) = 

    let selfName = actorNameVal
    let selfId = actorId
    let mutable selfTweets =  Array.create 0 ""
    let mutable receivedTweets = Array.create 0 ""
    let selfStopwatch = System.Diagnostics.Stopwatch()
    let mutable oldTime = 0.0
    let mutable alive = true

    let searchTweets receivedTweets searchString=
        let mutable searchedTweets = Array.empty
        for newTweets in receivedTweets do
            let mutable WordIndex = newTweets.IndexOf(searchString)
            if WordIndex <> -1 then
                searchedTweets <- Array.concat [| searchedTweets ; [|newTweets|] |]
        searchedTweets

    let rec loop() = actor {

        if oldTime - float(selfStopwatch.Elapsed.TotalSeconds) > 1.0 && not alive then
            oldTime <- float(selfStopwatch.Elapsed.TotalSeconds)
            alive <- true
            engineActorRef.[0] <! GoOnline selfId


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
            if timeNow - float(selfStopwatch.Elapsed.TotalMilliseconds) > 10.0 then
                mailbox.Self <! StartUser


        | GoOffline(myId) ->
            if alive then
                alive <- false
                oldTime <- float(selfStopwatch.Elapsed.TotalSeconds)

        | TweetInit ->
            let mutable randomMessageId = random.Next(tweets.Length)
            let mutable randomHashtagId = random.Next(hashtags.Length*2)
            let mutable mentionUserBoolean = random.Next(2)
            let mutable tweetString = tweets.[randomMessageId]
            if randomHashtagId < hashtags.Length then
                tweetString <- tweetString + hashtags.[randomHashtagId]
            if mentionUserBoolean = 1 then
                let mutable getnumNodes = engineActorRef.[0] <? getnumNodes
                let numNodes = Async.RunSynchronously (getnumNodes, 1000)
                let randomUserNameId = random.Next(numNodes)
                let mutable randomUserName = sprintf "@User%i" randomUserNameId
                tweetString <- tweetString + randomUserName
            selfTweets <- Array.concat [| selfTweets ; [|tweetString|] |]
            printfn "New Tweet from %s is %s" selfName tweetString
            engineActorRef.[0] <! Tweet(selfId, tweetString)

        | ReceiveTweet(newTweet) ->
            printfn "Received Tweet %s" newTweet
            receivedTweets <- Array.concat [| receivedTweets ; [|newTweet|] |]

        | RetweetInit ->
            engineActorRef.[0] <! Retweet(selfName)

        | RetweetReceive(newTweet) ->
            printfn "Retweet Tweet from %s is %s" selfName newTweet
            engineActorRef.[0] <! Tweet(selfId, newTweet)

        | SendRandomTweet(userName, userTweet) ->
            engineActorRef.[0] <! SendRandomTweet(userName, selfTweets.[random.Next(selfTweets.Length)])

        | SubscribeInit ->
            engineActorRef.[0] <! Subscribe(selfId)

        | QueryInit ->
            let mutable randomQueryId = random.Next(queries.Length)
            if queries.[randomQueryId] = "QuerySubscribedTweets" then
                mailbox.Self <! QuerySubscribedTweets
            elif queries.[randomQueryId] = "QueryHashtags" then
                mailbox.Self <! QueryHashtags(0, "")
            elif queries.[randomQueryId] = "QueryMentions" then
                mailbox.Self <! QueryMentions

        | QuerySubscribedTweets ->
            let mutable randomsearchString = search.[random.Next(search.Length)]
            let mutable tweetsFound = searchTweets receivedTweets randomsearchString
            printfn "Tweets found : %A" tweetsFound

        | QueryHashtags(someDummyNumber, someDummyString) ->
            engineActorRef.[0] <! QueryHashtags(selfId, hashtags.[random.Next(hashtags.Length)])

        | ReceiveQueryHashtags(tweetsFound) ->
            printfn "Tweets found : %A" tweetsFound

        | QueryMentions ->
            let mutable myMention = "@" + selfName
            let mutable tweetsFound = searchTweets receivedTweets myMention
            printfn "Tweets found : %A" tweetsFound

        |_ -> 0|>ignore


        return! loop()
    }
    loop ()

let MyengineActor (numNodesVal:int) (numTweetsVal:int) (mailbox : Actor<_>) = 

    let numNodes = numNodesVal 
    let numTweets = numTweetsVal
    let mutable userTweet = Map.empty
    let mutable subscribers = Map.empty
    let mutable tweetsToBeSent = Map.empty
    let mutable userTweetNumber = Array.empty
    let mutable offlineUser = Array.empty
    let mutable hashtagTweets = Map.empty
    let mutable tweetsReceived = 0
    let selfStopwatch = System.Diagnostics.Stopwatch()
    let mutable oldTime = 0.0

    let searchMentions newTweet=
        let mutable newUser = ""
        let mutable userTweetNumber = Array.empty
        let mutable userFound = 0
        for c in newTweet do
            if userFound = 0 && c = '@' then
                userFound <- 1
            elif userFound = 1 && c <> '@' then
                newUser <- newUser + string(c)
            elif userFound = 1 && (c = ' ' || c = '@' || c = '#') then
                userTweetNumber <- Array.concat [| userTweetNumber ; [|newUser|] |]
                userFound <- 0
                newUser <- ""
                if c = '@' then
                    userFound <- 1
        userTweetNumber

    let searchHashtags newTweet=
        let mutable newUser = ""
        let mutable userTweetNumber = Array.empty
        let mutable userFound = 0
        for c in newTweet do
            if userFound = 0 && c = '#' then
                userFound <- 1
            elif userFound = 1 && c <> '#' then
                newUser <- newUser + string(c)
            elif userFound = 1 && (c = ' ' || c = '@' || c = '#') then
                userTweetNumber <- Array.concat [| userTweetNumber ; [|newUser|] |]
                userFound <- 0
                newUser <- ""
                if c = '#' then
                    userFound <- 1
        userTweetNumber
                


    let rec loop() = actor {

        let! message = mailbox.Receive()
        let sender = mailbox.Sender()
        match message with

        | StartEngine ->

            userTweetNumber <- Array.zeroCreate (numNodes) 
            for i in 0..numNodes do
                let mutable workerName = sprintf "User%i" i
                userTweet <- userTweet.Add(workerName, [||])
                subscribers <- subscribers.Add(workerName, [||])
                tweetsToBeSent <- tweetsToBeSent.Add(workerName, [||])
            selfStopwatch.Start()

        | GetNumNodes ->
            sender <! numNodes

        | Tweet(UserId, tweetString) ->
            tweetsReceived <- tweetsReceived + 1
            if tweetsReceived > numTweets then
                ALL_COMPUTATIONS_DONE <- 1
            let mutable userName = (sprintf "User%i" UserId)
            let mutable allSubscribers = subscribers.[userName]
            let userMentions = searchMentions tweetString
            for mentioned in userMentions do
                allSubscribers <- allSubscribers |> Array.filter ((<>) mentioned )
                allSubscribers <- Array.concat [| allSubscribers ; [|mentioned|] |]

            let userHashtags = searchHashtags tweetString
            for hashtags in userHashtags do
                let mutable thisHashtagTweets = hashtagTweets.[hashtags]
                thisHashtagTweets <- thisHashtagTweets |> Array.filter ((<>) tweetString )
                thisHashtagTweets <- Array.concat [| thisHashtagTweets ; [|tweetString|] |]
                hashtagTweets <- hashtagTweets.Add(hashtags, thisHashtagTweets)

            for subs in allSubscribers do
                let mutable userName = (sprintf "User%i" subs)
                let mutable userFoundOffline = false
                for offlineUsersCurrent in offlineUsers do
                    if userFoundOffline = false then
                        if offlineUsersCurrent = Subs then
                            userFoundOffline = true
                if userFoundOffline = true then
                    let mutable usertweetsToBeSent = tweetsToBeSent.[userName]
                    usertweetsToBeSent <- usertweetsToBeSent |> Array.filter ((<>) tweetString )
                    usertweetsToBeSent <- Array.concat [| usertweetsToBeSent ; [|tweetString|] |]
                    tweetsToBeSent <- tweetsToBeSent.Add(Subs, usertweetsToBeSent)
                else
                    UserActorMapping.[userName] <! ReceiveTweet(tweetString)
                    
        | Retweet(userName) ->
            // choose a random user and ask them for a random tweet
            let mutable randomUser = random.Next(numNodes)
            UserActorMapping.[randomUser] <! SendRandomTweet(userName, "")

        | SendRandomTweet(userName, newUserTweet) ->
            UserActorMapping.[userName] <! RetweetReceive(newUserTweet)

        | Subscribe(UserId) ->

            let mutable allUsers = [|0..numNodes|]
            let mutable userName = (sprintf "User%i" UserId)
            let mutable userSubscribers = subscribers.[userName]
            if userSubscribers.Length < numNodes - 2 then
                // remove already subscribed indexes and choose from among the remaining ones
                for i in userSubscribers do
                    allUsers <- allUsers |> Array.filter ((<>) i )
                allUsers <- allUsers |> Array.filter ((<>) UserId )
                let mutable randomNewSub = random.Next(allUsers.Length)
                userSubscribers <- Array.concat [| userSubscribers ; [|randomNewSub|] |] 
                subscribers <- subscribers.Add(userName, userSubscribers)

        | GoOffline(UserId) ->
            offlineUsers <- offlineUsers |> Array.filter ((<>) UserId )
            offlineUsers <- Array.concat [| offlineUsers ; [|UserId|] |] 
            UserActorRef.[UserId] <! GoOffline(UserId)

        | GoOnline(UserId) ->
            let mutable userName = (sprintf "User%i" UserId)
            let mutable usertweetsToBeSent = tweetsToBeSent.[userName]
            offlineUsers <- offlineUsers |> Array.filter ((<>) UserId )
            for tweet in usertweetsToBeSent do
                UserActorMapping.[userName] <! ReceiveTweet(tweet)
            tweetsToBeSent <- tweetsToBeSent.Add(userName, [||])

        | QueryHashtags(UserId, HashtagQuery) ->
            let mutable userName = (sprintf "User%i" UserId)
            let mutable tweetsFound = hashtagTweets.[HashtagQuery]
            UserActorMapping.[userName] <! ReceiveQueryHashtags(tweetsFound)

        | _-> 0|>ignore 
        return! loop()
    }
    loop ()

let MybossActor (numNodesVal:int) (numTweetsVal:int) (mailbox : Actor<_>) = 

    let numNodes = numNodesVal 
    let numTweets = numTweetsVal
    let selfStopwatch = System.Diagnostics.Stopwatch()
    let mutable oldTime = 0.0

    let rec loop() = actor {

        let! message = mailbox.Receive()
        match message with

        | StartBoss ->

            // create Peers
            let engineActor = spawn system "engineActor" (MyengineActor numNodes numTweets)

            engineActorRef <- Array.zeroCreate 1
            engineActorRef.[0] <- engineActor
            engineActorRef.[0] <! StartEngine

            UserActorRef <- Array.zeroCreate (numNodes)

            for i in 0..numNodes-1 do
                let mutable workerName = (sprintf "User%i" i)
                let mutable userActor = spawn mailbox workerName (MyUserActor workerName i)
                userActor <! Register
                UserActorRef.[i] <- userActor
                UserActorMapping <- UserActorMapping.Add(workerName, i)

            // create Engine
            let engineActor = spawn system "engineActor" (MyengineActor numNodes numTweets)

            engineActorRef <- Array.zeroCreate 1
            engineActorRef.[0] <- engineActor

            // Send signal to all users to start their processes
            for i in 0..numNodes-1 do
                UserActorRef.[i] <! StartUser 
            mailbox.Self <! SimulateBoss
            selfStopwatch.Start()

        | SimulateBoss ->

            // choose random num/10 nodes and make them offline
            for i in 0..numNodes/10 do
                let mutable offlineNodeId = random.Next(numNodes)
                engineActorRef.[0] <! GoOffline(offlineNodeId)
                

            if oldTime - float(selfStopwatch.Elapsed.TotalSeconds) > 1.0 then
                oldTime <- float(selfStopwatch.Elapsed.TotalSeconds)
                mailbox.Self <! SimulateBoss
                
        | StopBoss ->
            ALL_COMPUTATIONS_DONE <- 1

        | _-> 0|>ignore 

        return! loop()
    }
    loop ()


// main function used to take in parameters
[<EntryPoint>]
let main argv =
    let numNodes = ((Array.get argv 1) |> int)
    let numTweets = ((Array.get argv 2) |> int)

    let bossActor = spawn system "bossActor" (MybossActor numNodes numTweets)

    bossActorRef <- Array.zeroCreate 1
    bossActorRef.[0] <- bossActor
    bossActorRef.[0] <! StartBoss

    while(ALL_COMPUTATIONS_DONE = 0) do
        0|>ignore

    system.Terminate() |> ignore
    0

//main fsi.CommandLineArgs