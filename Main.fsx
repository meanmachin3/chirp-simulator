#load "./Constants.fsx"
#load "./Logger.fsx"
#load "./MessageTypes.fsx"

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
    Configuration.parse @"akka {
            actor.provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            remote{
                dot-netty.tcp {
                    hostname = ""127.0.0.1""
                    port = 9001
                }
            }
        }"

let mutable TweetsExchanged = 0
let threshold = 10.0
let system = System.create "Twitter" config
let now = Diagnostics.Stopwatch()

let sendMessageToEngine data =
    let engine =
        select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system

    let json = Json.serialize data
    engine <! json

let sendMessageToUser data id =
    let user =
        select
            ("akka.tcp://Twitter@127.0.0.1:9001/user/User"
             + (id |> string))
            system

    let json = Json.serialize data
    user <! json

let UserActor (actorNameVal: string) (actorId: int) (mailbox: Actor<_>) =

    let name = actorNameVal
    let id = actorId
    let mutable liveTweets = Array.create 0 ""
    let mutable offlineTweets = Array.empty
    let timer = Diagnostics.Stopwatch()
    let mutable previousTimer = 0.0
    let mutable isOnline = true

    let rec loop () =
        actor {

            if float (timer.Elapsed.TotalSeconds)
               - previousTimer > 0.5
               && not isOnline then
                previousTimer <- float (timer.Elapsed.TotalSeconds)
                isOnline <- true

                let data =
                    { Author = id |> string
                      Message = ""
                      Operation = "getUpdates" }

                sendMessageToEngine (data)
                log Debug "Setting %d online" id


            let! json = mailbox.Receive()

            let message = Json.deserialize<TweetApiMessage> json

            log Debug "Actor called with %A" message

            mailbox.Sender() |> ignore

            let operation = message.Operation

            match operation with

            | "Register" ->
                log Debug "Registered: %s" name
                timer.Start()

            | "Run" ->
                // log Debug "Starting All Users"
                if isOnline then
                    let mutable actionId = random.Next(actions.Length)
                    let activity = actions.[actionId]
                    log Debug "Action selected =======> %s" actions.[actionId]
                    match activity with
                    | "tweet" ->
                        let data =
                            { Author = "" |> string
                              Message = ""
                              Operation = "Tweet" }

                        sendMessageToUser data id
                    | "retweet" ->
                        let data =
                            { Author = "" |> string
                              Message = ""
                              Operation = "Retweet" }

                        sendMessageToUser data id
                    | "subscribe" ->
                        let data =
                            { Author = "" |> string
                              Message = ""
                              Operation = "Subscribe" }

                        sendMessageToUser data id
                    | "query" ->
                        let data =
                            { Author = "" |> string
                              Message = ""
                              Operation = "Search" }

                        sendMessageToUser data id
                    | _ -> log Debug "Invalid Activity"
                let mutable timeNow = float (timer.Elapsed.TotalMilliseconds)
                while float (timer.Elapsed.TotalMilliseconds)
                      - timeNow < threshold do
                    0 |> ignore

                let data =
                    { Author = "" |> string
                      Message = ""
                      Operation = "Run" }

                sendMessageToUser data id


            | "Disconnect" ->
                if isOnline then
                    isOnline <- false
                    previousTimer <- float (timer.Elapsed.TotalSeconds)

            | "Tweet" ->
                // log Debug "<======== TweetInit =========>"
                let mutable mentionUserBoolean = random.Next(2)
                if mentionUserBoolean = 1 then
                    let data =
                        { Author = id |> string
                          Message = ""
                          Operation = "GetNumNodes" }

                    sendMessageToEngine data
                else
                    let mutable randomMessageId = random.Next(tweets.Length)
                    let mutable randomHashtagId = random.Next(hashtags.Length * 2)
                    if randomMessageId < tweets.Length then
                        let mutable tweetString = tweets.[randomMessageId]
                        if randomHashtagId < hashtags.Length
                        then tweetString <- tweetString + " " + hashtags.[randomHashtagId]
                        liveTweets <-
                            Array.concat [| liveTweets
                                            [| tweetString |] |]
                        log Debug "New Tweet from %s is %s" name tweetString

                        let data =
                            { Author = id |> string
                              Message = tweetString
                              Operation = "Tweet" }

                        sendMessageToEngine data
                        TweetsExchanged <- TweetsExchanged + 1
                        if TweetsExchanged >= totalTweetsToBeSent
                        then PROCESSING <- true

            | "GetNumNodes" ->
                let totalNodes = message.Message |> int
                let dummyId = message.Author
                let mutable randomMessageId = random.Next(tweets.Length)
                let mutable randomHashtagId = random.Next(hashtags.Length * 2)
                if randomMessageId < tweets.Length then
                    let mutable tweetString = tweets.[randomMessageId]
                    if randomHashtagId < hashtags.Length
                    then tweetString <- tweetString + " " + hashtags.[randomHashtagId]
                    let randomUserNameId = random.Next(totalNodes)
                    if randomUserNameId < totalNodes then
                        let mutable randomUserName = sprintf "@User%i" randomUserNameId
                        tweetString <- tweetString + " " + randomUserName
                    liveTweets <-
                        Array.concat [| liveTweets
                                        [| tweetString |] |]
                    log Debug "New Tweet from %s is %s" name tweetString

                    let data =
                        { Author = id |> string
                          Message = tweetString
                          Operation = "Tweet" }

                    sendMessageToEngine data

            | "getUpdates" ->
                let newTweet = message.Message
                log Debug "Received Tweet %A" newTweet
                offlineTweets <-
                    Array.concat [| offlineTweets
                                    [| newTweet |] |]

            | "Retweet" ->
                // log Debug "<======== RetweetInit =========>"
                let data =
                    { Author = id |> string
                      Message = ""
                      Operation = "Retweet" }

                sendMessageToEngine data

            | "RetweetReceive" ->
                let newTweet = message.Message
                log Debug "Retweet from %s is %s" name newTweet

                let data =
                    { Author = id |> string
                      Message = newTweet
                      Operation = "Tweet" }

                sendMessageToEngine data

            | "Subscribe" ->
                // log Debug "<======= SubscribeInit =======>"
                let data =
                    { Author = id |> string
                      Message = ""
                      Operation = "Subscribe" }

                sendMessageToEngine data

            | "Search" ->
                // log Debug "<======= QueryInit =======>"
                let mutable randomQueryId = random.Next(queries.Length)
                if randomQueryId < queries.Length then
                    if queries.[randomQueryId] = "searchTweets" then
                        let data =
                            { Author = id |> string
                              Message = ""
                              Operation = "searchTweets" }

                        let json = Json.serialize data
                        mailbox.Self <! json
                    elif queries.[randomQueryId] = "searchForHashtag" then
                        let data =
                            { Author = id |> string
                              Message = ""
                              Operation = "searchForHashtag" }

                        let json = Json.serialize data
                        mailbox.Self <! json
                    elif queries.[randomQueryId] = "searchForMention" then
                        let data =
                            { Author = id |> string
                              Message = ""
                              Operation = "searchForMention" }

                        let json = Json.serialize data
                        mailbox.Self <! json




            | "searchTweets" ->
                // log Debug "<======= searchTweets =======>"
                let mutable randomSearchId = random.Next(search.Length)
                if randomSearchId < search.Length then
                    let mutable randomsearchString = search.[randomSearchId]

                    let data =
                        { Author = id |> string
                          Message = randomsearchString
                          Operation = "searchTweets" }

                    sendMessageToEngine data

            | "getSearchTweets" ->
                // log Debug "<======= getSearchTweets =======>"
                let searchString = message.Author
                let searchTweetResults = message.Message
                log Debug "searchTweets with search = %s found : %s" searchString searchTweetResults


            | "searchForHashtag" ->
                // log Debug "<======= searchForHashtag =======>"
                let mutable randomHashtagId = random.Next(hashtags.Length)
                if randomHashtagId < hashtags.Length then
                    let data =
                        { Author = id |> string
                          Message = hashtags.[randomHashtagId]
                          Operation = "searchForHashtag" }

                    sendMessageToEngine data

            | "getSearchForHashtag" ->
                // log Debug "<======= getSearchForHashtag =======>"
                let searchHashtag = message.Author
                let tweetsFound = message.Message
                log Debug "getSearchForHashtag with search = %s found : %A" searchHashtag tweetsFound


            | "searchForMention" ->
                // log Debug "<======= searchForMention =======>"

                let data =
                    { Author = id |> string
                      Message = ""
                      Operation = "searchForMention" }

                sendMessageToEngine data

            | "getSearchForMention" ->
                // log Debug "<======= getSearchForMention =======>"
                let tweetsFound = message.Message
                log Debug "getSearchForMention found : %A" tweetsFound


            | _ -> log Debug "Invalid Request"


            return! loop ()
        }

    loop ()


let Supervisor (nodes: int) (tweets: int) (mailbox: Actor<_>) =

    let totalNodes = nodes
    let totalTweets = tweets
    let mutable prev = 0.0

    let rec loop () =
        actor {
            let! message = mailbox.Receive()
            // log Debug "<======= Supervisor =======>"

            match message with
            | "Start" ->
                // log Debug "<======= Start Supervisor =======>"
                [ 0 .. totalNodes - 1 ]
                |> List.iter (fun i ->
                    let mutable name = sprintf "User%i" i
                    let mutable actor = spawn system name (UserActor name i)

                    let data =
                        { Author = ""
                          Message = name
                          Operation = "Register" }

                    sendMessageToUser data i)

                now.Start()
                prev <- float (now.Elapsed.TotalSeconds)

                while float (now.Elapsed.TotalSeconds) - prev < 1.0 do
                    "" |> ignore

                [ 0 .. totalNodes - 1 ]
                |> List.iter (fun i ->
                    let data =
                        { Author = "" |> string
                          Message = ""
                          Operation = "Run" }

                    sendMessageToUser data i)
                log Debug "Done with users"

                mailbox.Self <! "Run"


            | "Run" ->
                [ 0 .. 1 ]
                |> List.iter (fun i ->
                    let mutable offlineNodeId = random.Next(totalNodes)
                    log Debug "Setting %i offline" offlineNodeId

                    let data =
                        { Author = offlineNodeId |> string
                          Message = ""
                          Operation = "Disconnect" }

                    sendMessageToEngine data)

                while float (now.Elapsed.TotalSeconds) - prev < 1.0 do
                    0 |> ignore
                prev <- float (now.Elapsed.TotalSeconds)
                mailbox.Self <! "Run"

            | _ -> log Debug "Invalid Command"

            return! loop ()
        }

    loop ()

//[<EntryPoint>]
let main argv =

    let numNodes =
        int (string (fsi.CommandLineArgs.GetValue 1))

    let numTweets =
        int (string (fsi.CommandLineArgs.GetValue 2))

    totalTweetsToBeSent <- numTweets

    let bossActor =
        spawn system "bossActor" (Supervisor numNodes numTweets)

    let destinationRef =
        select ("akka.tcp://Twitter@127.0.0.1:9001/user/bossActor") system

    destinationRef <! "Start"

    while (PROCESSING = false) do
        0 |> ignore

    now.Stop()

    log Debug "Time spent in processing %f" now.Elapsed.TotalMilliseconds
    system.Terminate() |> ignore
    0

main fsi.CommandLineArgs
