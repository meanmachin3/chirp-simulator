#r "nuget: Akka.FSharp" 

open Akka.FSharp

module Constants =

    /// Log levels.
    let Error = 0
    let Warning = 1
    let Information = 2
    let Debug = 3


    let mutable ALL_COMPUTATIONS_DONE = 0
    let actions = [|"tweet"; "subscribe"; "retweet"; "query"|]
    let queries = [|"QuerySubscribedTweets"; "QueryHashtags"; "QueryMentions"|]
    let hashtags = [|"#COP5615isgreat"; "#FSharp"; "#Pikachu"; "#Pokemon"; "#GoGators"; "#MarstonLibrary"|]
    let search = [|"DOS"; "Coding"; "Pokemon"; "UF"; "Guess"|]
    let tweets = [|"Doing DOS rn, talk later!"; "Coding takes time!"; "Watching Pokemon!"; "Playing Pokemon Go!" ; "UF is awesome!"; "Guess what?"|]
    let userRegexMatch = "User([0-9]*)"
    let random = System.Random()
    let mutable totalTweetsToBeSent = 0