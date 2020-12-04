#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#r "nuget: FSharp.Json"

#load "./Constants.fsx"
#load "./MessageTypes.fsx"
#load "./Logger.fsx"
#load "./Utils.fsx"

open Constants.Constants
open System
open Akka.FSharp
open Akka.Actor
open Akka.Configuration
open MessageTypes
open FSharp.Json
open Logger.Logging
open Utils.TwitterEngine

let config =
    Configuration.parse @"akka {
            actor.provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            remote{
                dot-netty.tcp {
                    hostname = ""127.0.0.1""
                    port = 9002
                }
                handshake-timeout = 100 s

            }
        }"

let system = System.create "Twitter" config


let sendMessageToEngine data =
    let destinationRef =
        select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system

    let json = Json.serialize data
    destinationRef <! json

let sendMessageToUser data id =
    let destinationRef =
        select
            ("akka.tcp://Twitter@127.0.0.1:9001/user/User"
             + (id |> string))
            system

    let json = Json.serialize data
    destinationRef <! json

let generateUserName id = sprintf "User%i" id

let TwitterEngine (numNodesVal: int) (numTweetsVal: int) (mailbox: Actor<_>) =

    let numNodes = numNodesVal
    let numTweets = numTweetsVal
    let mutable following = Map.empty
    let mutable offlineTweets = Map.empty
    let mutable tweets = Map.empty
    let mutable followersTweet = Map.empty
    let mutable tweetsMentions = Map.empty
    let mutable loggedOutUsers = [||]
    let mutable tweetsHastags = Map.empty
    let mutable tweetsReceived = 0
    let timer = Diagnostics.Stopwatch()

    let rec loop () =
        actor {
            // log Debug "Actor called"
            let! json = mailbox.Receive()
            let message = Json.deserialize<TweetApiMessage> json
            // log Debug "Actor called with %A" message

            let sender = mailbox.Sender()

            let operation = message.Operation

            match operation with

            | "Run" ->
                // log Debug "Run"
                for i in 0 .. numNodes - 1 do
                    let mutable workerName = generateUserName i
                    following <- following.Add(workerName, [| -1 |])
                    offlineTweets <- offlineTweets.Add(workerName, [| { Author = ""; Message = "" } |])
                    tweets <- tweets.Add(workerName, [| "" |])
                    tweetsMentions <- tweetsMentions.Add(workerName, [| "" |])
                    followersTweet <- followersTweet.Add(workerName, [| "" |])
                tweetsHastags <- tweetsHastags.Add("", [| "" |])
                timer.Start()

            | "GetNumNodes" ->
                // log Debug "GetNumNodes"
                let userId = message.Author |> int

                let destinationRef =
                    select
                        ("akka.tcp://Twitter@127.0.0.1:9001/user/User"
                         + (userId |> string))
                        system

                let data =
                    { Author = userId |> string
                      Message = numNodes |> string
                      Operation = "GetNumNodes" }

                let json = Json.serialize data
                destinationRef <! json

            | "Tweet" ->
                log Debug "Tweet"
                let userId = message.Author |> int
                let tweetString = message.Message
                tweetsReceived <- tweetsReceived + 1
                log Debug "Tweets received = %d" tweetsReceived
                if tweetsReceived > numTweets then PROCESSING <- true
                if userId < numNodes then
                    let mutable userName = generateUserName userId

                    let mutable tweetsByUser = tweets.[userName]
                    tweetsByUser <-
                        Array.concat [| tweetsByUser
                                        [| tweetString |] |]
                    tweets <- tweets.Add(userName, tweetsByUser)

                    let userHashtags = searchFor Hashtag tweetString
                    for hashtags in userHashtags do
                        if tweetsHastags.ContainsKey(hashtags) then
                            let mutable thistweetsHastags = tweetsHastags.[hashtags]
                            thistweetsHastags <-
                                Array.concat [| thistweetsHastags
                                                [| tweetString |] |]
                            tweetsHastags <- tweetsHastags.Add(hashtags, thistweetsHastags)
                        else
                            tweetsHastags <- tweetsHastags.Add(hashtags, [| tweetString |])

                    let userMentions = searchFor Mention tweetString
                    for mentioned in userMentions do
                        let mutable myMentionedTweets = tweetsMentions.[mentioned]
                        myMentionedTweets <-
                            Array.concat [| myMentionedTweets
                                            [| tweetString |] |]
                        tweetsMentions <- tweetsMentions.Add(mentioned, myMentionedTweets)

                    let mutable allfollowing = following.[userName]
                    allfollowing <- allfollowing |> Array.filter ((<>) -1)

                    for mentioned in userMentions do
                        let mentionedId = matchSample userRegexMatch mentioned
                        if mentionedId < numNodes then
                            allfollowing <- allfollowing |> Array.filter ((<>) mentionedId)
                            allfollowing <-
                                Array.concat [| allfollowing
                                                [| mentionedId |] |]

                    for subs in allfollowing do
                        let mutable destination = generateUserName subs
                        let mutable newUserSubTweets = followersTweet.[destination]
                        newUserSubTweets <-
                            Array.concat [| newUserSubTweets
                                            [| tweetString |] |]
                        followersTweet <- followersTweet.Add(destination, newUserSubTweets)

                        let mutable userFoundOffline = false

                        let tweet =
                            { Author = userName
                              Message = tweetString }

                        for loggedOutUsersCurrent in loggedOutUsers do
                            if not userFoundOffline
                            then if loggedOutUsersCurrent = subs then userFoundOffline <- true
                        if userFoundOffline then
                            let mutable userofflineTweets = offlineTweets.[destination]
                            userofflineTweets <-
                                Array.concat [| userofflineTweets
                                                [| tweet |] |]
                            offlineTweets <- offlineTweets.Add(destination, userofflineTweets)
                        else
                            let destinationRef =
                                select
                                    ("akka.tcp://Twitter@127.0.0.1:9001/user/User"
                                     + (subs |> string))
                                    system

                            let data =
                                { Author = userName |> string
                                  Message = tweetString
                                  Operation = "getUpdates" }

                            let json = Json.serialize data
                            destinationRef <! json

            | "Retweet" ->
                log Debug "Retweet"
                let id = message.Author |> int
                let mutable rnd = random.Next(numNodes)
                let mutable name = generateUserName rnd
                let allRandomUserTweets = tweets.[name]
                let randomTweetNumber = random.Next(allRandomUserTweets.Length)
                if randomTweetNumber < allRandomUserTweets.Length then
                    let destinationRef =
                        select
                            ("akka.tcp://Twitter@127.0.0.1:9001/user/User"
                             + (id |> string))
                            system

                    let data =
                        { Author = "" |> string
                          Message = allRandomUserTweets.[randomTweetNumber]
                          Operation = "getUpdates" }

                    let json = Json.serialize data
                    destinationRef <! json

            | "Subscribe" ->
                log Debug "Subscribe"
                let userId = message.Author |> int
                let mutable allUsers = [| 0 .. numNodes - 1 |]
                let mutable userName = sprintf "User%i" userId
                if userId < numNodes then
                    let mutable userfollowing = following.[userName]
                    userfollowing <- userfollowing |> Array.filter ((<>) -1)
                    if userfollowing.Length < numNodes - 2 then
                        for i in userfollowing do
                            allUsers <- allUsers |> Array.filter ((<>) i)
                        allUsers <- allUsers |> Array.filter ((<>) userId)
                        let mutable randomNewSub = random.Next(allUsers.Length)
                        userfollowing <-
                            Array.concat [| userfollowing
                                            [| allUsers.[randomNewSub] |] |]
                        following <- following.Add(userName, userfollowing)
                        log Debug "%d is subscribing to %d" userId randomNewSub

            | "Disconnect" ->
                log Debug "GoOffline"
                let userId = message.Author |> int
                loggedOutUsers <- loggedOutUsers |> Array.filter ((<>) userId)
                loggedOutUsers <-
                    Array.concat [| loggedOutUsers
                                    [| userId |] |]

                let destinationRef =
                    select
                        ("akka.tcp://Twitter@127.0.0.1:9001/user/User"
                         + (userId |> string))
                        system

                let data =
                    { Author = userId |> string
                      Message = ""
                      Operation = "Disconnect" }

                let json = Json.serialize data
                destinationRef <! json

            | "getUpdates" ->
                log Debug "GoOnline"
                let userId = message.Author |> int
                let mutable userName = sprintf "User%i" userId
                if userId < numNodes then
                    let mutable userofflineTweets = offlineTweets.[userName]
                    loggedOutUsers <- loggedOutUsers |> Array.filter ((<>) userId)
                    for tweet in userofflineTweets do
                        let destinationRef =
                            select
                                ("akka.tcp://Twitter@127.0.0.1:9001/user/User"
                                 + (userId |> string))
                                system

                        let data =
                            { Author = tweet.Author |> string
                              Message = tweet.Message
                              Operation = "getUpdates" }

                        let json = Json.serialize data
                        destinationRef <! json
                    offlineTweets <- offlineTweets.Add(userName, [| { Author = ""; Message = "" } |])

            | "searchTweets" ->
                log Debug "QuerySubscribedTweets"
                let userId = message.Author |> int
                let searchString = message.Message
                let mutable userName = sprintf "User%i" userId
                let mutable newUserSubTweets = followersTweet.[userName]
                newUserSubTweets <- newUserSubTweets |> Array.filter ((<>) "")

                let mutable tweetsFound =
                    searchTweets newUserSubTweets searchString

                let destinationRef =
                    select
                        ("akka.tcp://Twitter@127.0.0.1:9001/user/User"
                         + (userId |> string))
                        system

                if tweetsFound.Length <> 0 then
                    let arr = Json.serialize tweetsFound

                    let data =
                        { Author = searchString |> string
                          Message = arr
                          Operation = "getSearchTweets" }

                    let json = Json.serialize data
                    destinationRef <! json
                else
                    let arr = Json.serialize [| "No tweets found" |]

                    let data =
                        { Author = searchString |> string
                          Message = arr
                          Operation = "getSearchTweets" }

                    let json = Json.serialize data
                    destinationRef <! json

            | "searchForHashtag" ->
                log Debug "QueryHashtags"
                let userId = message.Author |> int
                let hashtagQuery = message.Message
                if userId < numNodes then
                    let hashtagString = stripchars "#" hashtagQuery

                    let destinationRef =
                        select
                            ("akka.tcp://Twitter@127.0.0.1:9001/user/User"
                             + (userId |> string))
                            system

                    if tweetsHastags.ContainsKey(hashtagString) then
                        let mutable tweetsFound = tweetsHastags.[hashtagString]
                        let arr = Json.serialize tweetsFound

                        let data =
                            { Author = hashtagQuery |> string
                              Message = arr
                              Operation = "getSearchForHashtag" }

                        let json = Json.serialize data
                        destinationRef <! json
                    else
                        let arr = Json.serialize [| "No tweets found" |]

                        let data =
                            { Author = hashtagQuery |> string
                              Message = arr
                              Operation = "getSearchForHashtag" }

                        let json = Json.serialize data
                        destinationRef <! json

            | "searchForMention" ->
                log Debug "QueryMentions"
                let userId = message.Author |> int
                let mutable userName = sprintf "User%i" userId
                let mutable userMentions = tweetsMentions.[userName]

                let destinationRef =
                    select
                        ("akka.tcp://Twitter@127.0.0.1:9001/user/User"
                         + (userId |> string))
                        system

                if userMentions.Length <> 0 then
                    let arr = Json.serialize userMentions

                    let data =
                        { Author = "" |> string
                          Message = arr
                          Operation = "getSearchForMention" }

                    let json = Json.serialize data
                    destinationRef <! json
                else
                    let arr = Json.serialize [| "No tweets found" |]

                    let data =
                        { Author = "" |> string
                          Message = arr
                          Operation = "getSearchForMention" }

                    let json = Json.serialize data
                    destinationRef <! json

            | _ ->
                log Debug "Invalid Request"
                sender <! "Invalid Request"

            return! loop ()
        }

    loop ()

let main argv =
    let numNodes =
        int (string (fsi.CommandLineArgs.GetValue 1))

    let numTweets =
        int (string (fsi.CommandLineArgs.GetValue 2))

    let engineActor =
        spawn system "engineActor" (TwitterEngine numNodes numTweets)

    let destinationRef =
        select ("akka.tcp://Twitter@127.0.0.1:9002/user/engineActor") system

    let data =
        { Author = ""
          Message = ""
          Operation = "Run" }

    let json = Json.serialize data
    destinationRef <! json

    // log Debug "Done with engineActor"

    while (PROCESSING = false) do
        0 |> ignore

    system.Terminate() |> ignore
    0

main fsi.CommandLineArgs
