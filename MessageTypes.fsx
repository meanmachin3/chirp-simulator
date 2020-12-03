


namespace MessageTypes
    
    type TweetMessage = {
        Author: string
        Message: string
    }
    type TweetApiMessage = {
        Author: string
        Message: string
        Operation: string
    }
    type Messages =
        | StartBoss
        | SimulateBoss