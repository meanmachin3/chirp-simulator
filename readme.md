<h1 align="center"> Twitter Simulator </h1> <br>

## Table of Contents

- [Table of Contents](#table-of-contents)
- [Build Process](#build-process)
- [What is Working](#what-is-working)
- [Implementation](#implementation)
- [Simulation](#simulation)
- [Performance](#performance)

## Build Process

- Download the repository `git clone https://github.com/meanmachin3/gossip-protocol.git`
- `dotnet fsi --langversion:preview RemoteActor.fsx numNodes numTweets` to run Twitter Engine script where `numNodes` is the number of users you want to run simulator/tester for. `numTweets` is the amount of tweets that needs to simulated.
- `dotnet fsi --langversion:preview Main.fsx numNodes numTweets` to run User Engine script where `numNodes` is the number of users you want to run simulator/tester for. `numTweets` is the amount of tweets that needs to simulated.

## What is Working

- Register account
- Send tweet. Tweets can have hashtags (e.g. #COP5615isgreat) and mentions (@bestuser)
- Subscribe to user's tweets
- Re-tweets (so that your subscribers get an interesting tweet you got by other means)
- Allow querying tweets subscribed to, tweets with specific hashtags, tweets in which the user is mentioned (my mentions)
- If the user is connected, deliver the above types of tweets live (without querying)

## Implementation

Twitter Engine and User Engine accepts command line parameters as `numNodes` that denotes the amount of user that needs to be simulated along with `numTweets` that tells how many tweets needs to exchanged. There's no need of iteraction with the user engine actor as it simulates real life scenario of tweet, retweeting, subscribing and searching for tweets.

Once the Twitter Engine is started, it waits for incoming connection request. 

## Simulation

## Performance