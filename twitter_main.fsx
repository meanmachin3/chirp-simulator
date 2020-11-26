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
| Retweet of int
| RetweetReceive of string
| Subscribe of int
| SubscribeInit
| QueryInit
| QuerySubscribedTweets of int*string
| ReceiveQuerySubscribedTweets of string*string[]
| QueryHashtags of int*string
| ReceiveQueryHashtags of string*string[]
| QueryMentions of int
| ReceiveQueryMentions of string[]
| DeliverTweet of string[]
| GoOffline of int
| GoOnline of int
| GetNumNodes of int*int


let actions = [|"tweet"; "tweet"; "subscribe"; "retweet"; "query"|]
let queries = [|"QuerySubscribedTweets"; "QueryHashtags"; "QueryMentions"|]
let hashtags = [|"#COP5615isgreat"; "#FSharp"; "#Pikachu"; "#Pokemon"; "#GoGators"; "#MarstonLibrary"|]
let search = [|"DOS"; "Coding"; "Pokemon"; "UF"; "Guess"|]
let tweets = [|"Doing DOS rn, talk later!"; "Coding takes time!"; "Watching Pokemon!"; "Playing Pokemon Go!" ; "UF is awesome!"; "Guess what?"|]
let userRegexMatch = "User([0-9]*)"
let random = System.Random()
let config =
    Configuration.parse
        @"akka {
                log-dead-letters = off
            }
        }"
let system = System.create "system" (config)
//<| ConfigurationFactory.Default()


let MyUserActor (actorNameVal:string) (actorId:int) (mailbox : Actor<_>) = 

    let selfName = actorNameVal
    let selfId = actorId
    let mutable selfTweets =  Array.create 0 ""
    let mutable receivedTweets = Array.create 0 ""
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
            printfn "Received Tweet %s" newTweet
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

let MyengineActor (numNodesVal:int) (numTweetsVal:int) (mailbox : Actor<_>) = 

    let numNodes = numNodesVal 
    let numTweets = numTweetsVal
    let mutable subscribers = Map.empty
    let mutable tweetsToBeSent = Map.empty
    let mutable allTweets = Map.empty
    let mutable userSubscribedTweets = Map.empty
    let mutable myMentions = Map.empty
    let mutable offlineUsers = Array.empty
    let mutable hashtagTweets = Map.empty
    let mutable tweetsReceived = 0
    let selfStopwatchEngine = System.Diagnostics.Stopwatch()

    let searchMentions newTweet=
        let mutable newUser = ""
        let mutable userTweetNumber = Array.create 0 ""
        let mutable userFound = 0
        for c in newTweet do
            if userFound = 0 && c = '@' then
                userFound <- 1
            elif userFound = 1 && (c = ' ' || c = '@' || c = '#') then
                newUser <- newUser.Trim()
                if newUser <> "" then
                    userTweetNumber <- Array.concat [| userTweetNumber ; [|newUser|] |]
                userFound <- 0
                newUser <- ""
                if c = '@' then
                    userFound <- 1
            elif userFound = 1 && c <> '@' then
                newUser <- newUser + string(c)
        newUser <- newUser.Trim()
        if newUser <> "" then
            userTweetNumber <- Array.concat [| userTweetNumber ; [|newUser|] |]
        userTweetNumber

    let searchHashtags newTweet=
        let mutable newUser = ""
        let mutable userTweetNumber = Array.create 0 ""
        let mutable userFound = 0
        for c in newTweet do
            if userFound = 0 && c = '#' then
                userFound <- 1
            elif userFound = 1 && (c = ' ' || c = '@' || c = '#') then
                newUser <- newUser.Trim()
                if newUser <> "" then
                    userTweetNumber <- Array.concat [| userTweetNumber ; [|newUser|] |]
                userFound <- 0
                newUser <- ""
                if c = '#' then
                    userFound <- 1
            elif userFound = 1 && c <> '#' then
                newUser <- newUser + string(c)
        newUser <- newUser.Trim()
        if newUser <> "" then
            userTweetNumber <- Array.concat [| userTweetNumber ; [|newUser|] |]
        userTweetNumber

    let searchTweets (receivedTweets:string[]) (searchString:string)=
        let mutable searchedTweets = Array.create 0 ""
        for newTweets in receivedTweets do
            let mutable wordIndex = newTweets.IndexOf(searchString)
            if wordIndex <> -1 then
                searchedTweets <- Array.concat [| searchedTweets ; [|newTweets|] |]
        searchedTweets
                
    let matchSample r m =
        let r = Regex(r)
        let m1 = r.Match m
        let idFound = m1.Groups.[1] |> string |> int
        idFound

    let stripchars chars str =
        Seq.fold
            (fun (str: string) chr ->
            str.Replace(chr |> Char.ToUpper |> string, "").Replace(chr |> Char.ToLower |> string, ""))
            str chars

    let rec loop() = actor {

        let! message = mailbox.Receive()
        let sender = mailbox.Sender()
        match message with

        | StartEngine ->

            for i in 0..numNodes-1 do
                let mutable workerName = sprintf "User%i" i
                subscribers <- subscribers.Add(workerName, [|-1|])
                tweetsToBeSent <- tweetsToBeSent.Add(workerName, [|""|])
                allTweets <- allTweets.Add(workerName, [|""|])
                myMentions <- myMentions.Add(workerName, [|""|])
                userSubscribedTweets <- userSubscribedTweets.Add(workerName, [|""|])
            hashtagTweets <- hashtagTweets.Add("", [|""|])
            selfStopwatchEngine.Start()

        | GetNumNodes(dummyValue, userId) ->
            let destinationRef = select ("akka://system/user/User"+ (userId |> string)) system
            destinationRef <! GetNumNodes(numNodes, userId)

        | Tweet(userId, tweetString) ->
            tweetsReceived <- tweetsReceived + 1
            printfn "Tweets received = %d" tweetsReceived
            if tweetsReceived > numTweets then
                ALL_COMPUTATIONS_DONE <- 1
            if userId < numNodes then
                let mutable userName = sprintf "User%i" userId

                // store tweet for user
                let mutable allTweetsByUser = allTweets.[userName]
                allTweetsByUser <- Array.concat [| allTweetsByUser ; [|tweetString|] |]
                allTweets <- allTweets.Add(userName, allTweetsByUser)

                // store hashtags for tweet
                let userHashtags = searchHashtags tweetString
                for hashtags in userHashtags do
                    if hashtagTweets.ContainsKey(hashtags) then
                        let mutable thisHashtagTweets = hashtagTweets.[hashtags]
                        thisHashtagTweets <- Array.concat [| thisHashtagTweets ; [|tweetString|] |]
                        hashtagTweets <- hashtagTweets.Add(hashtags, thisHashtagTweets)
                    else
                        hashtagTweets <- hashtagTweets.Add(hashtags, [|tweetString|]) 

                // store user mentions for tweet
                let userMentions = searchMentions tweetString
                for mentioned in userMentions do
                    let mutable myMentionedTweets = myMentions.[mentioned]
                    myMentionedTweets <- Array.concat [| myMentionedTweets ; [|tweetString|] |]
                    myMentions <- myMentions.Add(mentioned, myMentionedTweets)

                // who should this tweet be sent out to?
                let mutable allSubscribers = subscribers.[userName]
                allSubscribers <- allSubscribers |> Array.filter ((<>) -1 )
                
                for mentioned in userMentions do
                    let mentionedId = matchSample userRegexMatch mentioned
                    if mentionedId < numNodes then
                        allSubscribers <- allSubscribers |> Array.filter ((<>) mentionedId )
                        allSubscribers <- Array.concat [| allSubscribers ; [|mentionedId|] |]

                for subs in allSubscribers do
                    let mutable userName = (sprintf "User%i" subs)
                    // store in user subscribed tweets
                    let mutable newUserSubTweets = userSubscribedTweets.[userName]
                    newUserSubTweets <- Array.concat [| newUserSubTweets ; [|tweetString|] |]
                    userSubscribedTweets <- userSubscribedTweets.Add(userName, newUserSubTweets)

                    // should we send it or not?
                    let mutable userFoundOffline = false
                    for offlineUsersCurrent in offlineUsers do
                        if not userFoundOffline then
                            if offlineUsersCurrent = subs then
                                userFoundOffline <- true
                    if userFoundOffline then
                        let mutable usertweetsToBeSent = tweetsToBeSent.[userName]
                        usertweetsToBeSent <- usertweetsToBeSent |> Array.filter ((<>) tweetString )
                        usertweetsToBeSent <- usertweetsToBeSent |> Array.filter ((<>) "" )
                        usertweetsToBeSent <- Array.concat [| usertweetsToBeSent ; [|tweetString|] |]
                        tweetsToBeSent <- tweetsToBeSent.Add(userName, usertweetsToBeSent)
                    else
                        let destinationRef = select ("akka://system/user/User"+ (subs |> string)) system
                        destinationRef <! ReceiveTweet(tweetString)
                    
        | Retweet(userId) ->
            // choose a random user and ask them for a random tweet
            let mutable randomUserId = random.Next(numNodes)
            let mutable randomUserName = sprintf "User%i" randomUserId
            let allRandomUserTweets = allTweets.[randomUserName]
            let randomTweetNumber = random.Next(allRandomUserTweets.Length)
            if randomTweetNumber < allRandomUserTweets.Length then
                let destinationRef = select ("akka://system/user/User"+ (userId |> string)) system
                destinationRef <! RetweetReceive(allRandomUserTweets.[randomTweetNumber])

        | Subscribe(userId) ->

            let mutable allUsers = [|0..numNodes-1|]
            let mutable userName = sprintf "User%i" userId
            if userId < numNodes then
                let mutable userSubscribers = subscribers.[userName]
                userSubscribers <- userSubscribers |> Array.filter ((<>) -1 )
                if userSubscribers.Length < numNodes - 2 then
                    // remove already subscribed indexes and choose from among the remaining ones
                    for i in userSubscribers do
                        allUsers <- allUsers |> Array.filter ((<>) i )
                    allUsers <- allUsers |> Array.filter ((<>) userId )
                    let mutable randomNewSub = random.Next(allUsers.Length)
                    userSubscribers <- Array.concat [| userSubscribers ; [|randomNewSub|] |] 
                    subscribers <- subscribers.Add(userName, userSubscribers)
                    printfn "%d is subscribing to %d" userId randomNewSub

        | GoOffline(userId) ->
            offlineUsers <- offlineUsers |> Array.filter ((<>) userId )
            offlineUsers <- Array.concat [| offlineUsers ; [|userId|] |] 
            let destinationRef = select ("akka://system/user/User"+ (userId |> string)) system
            destinationRef <! GoOffline(userId)

        | GoOnline(userId) ->
            let mutable userName = sprintf "User%i" userId
            if userId < numNodes then
                let mutable usertweetsToBeSent = tweetsToBeSent.[userName]
                usertweetsToBeSent <- usertweetsToBeSent |> Array.filter ((<>) "" )
                offlineUsers <- offlineUsers |> Array.filter ((<>) userId )
                for tweet in usertweetsToBeSent do
                    let destinationRef = select ("akka://system/user/User"+ (userId |> string)) system
                    destinationRef <! ReceiveTweet(tweet)
                tweetsToBeSent <- tweetsToBeSent.Add(userName, [|""|])

        | QuerySubscribedTweets(userId, searchString) ->
            let mutable userName = sprintf "User%i" userId
            let mutable newUserSubTweets = userSubscribedTweets.[userName]
            newUserSubTweets <- newUserSubTweets |> Array.filter ((<>) "" )
            let mutable tweetsFound = searchTweets newUserSubTweets searchString
            let destinationRef = select ("akka://system/user/User"+ (userId |> string)) system
            if tweetsFound.Length <> 0 then
                destinationRef <! ReceiveQuerySubscribedTweets(searchString, tweetsFound)
            else 
                destinationRef <! ReceiveQuerySubscribedTweets(searchString, [| "No tweets found" |])

        | QueryHashtags(userId, hashtagQuery) ->
            if userId < numNodes then
                let hashtagString = stripchars "#" hashtagQuery
                let destinationRef = select ("akka://system/user/User"+ (userId |> string)) system
                if hashtagTweets.ContainsKey(hashtagString) then
                    let mutable tweetsFound = hashtagTweets.[hashtagString]
                    destinationRef <! ReceiveQueryHashtags(hashtagQuery, tweetsFound)
                else
                    destinationRef <! ReceiveQueryHashtags(hashtagQuery, [| "No tweets found" |])

        | QueryMentions(userId) ->
            let mutable userName = sprintf "User%i" userId
            let mutable userMentions = myMentions.[userName]
            let destinationRef = select ("akka://system/user/User"+ (userId |> string)) system
            if userMentions.Length <> 0 then
                destinationRef <! ReceiveQueryMentions(userMentions)
            else
                destinationRef <! ReceiveQueryMentions([| "No tweets found" |])

        | _-> 0|>ignore 
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