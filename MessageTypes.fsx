


namespace MessageTypes
    
    type TweetMessage = {
        Author: string
        Message: string
    }
    type ApiMessage = {
        Author: string
        Payload: string
        Operation: string
    }

    type TweetApiMessage = {
        Author: string
        Message: string
        Operation: string
    }
    type Messages =
        | Join of int*int
        | StartBoss
        | SimulateBoss
        | StopBoss
        | StartEngine
        | Register
        | StartUser
        | TweetInit
        | Tweet of int*string
        | ReceiveTweet of TweetMessage
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
        | Ping of string