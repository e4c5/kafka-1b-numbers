The 1 Billion Message Challenge
=
I recently gave some of my proteges a challenge to send 1 billion messages through Kafka and for those messages to be processed and counted through a consumer in the shortest possible time. The messages were actually integers between 1 and 1,000,000.
I intentionally worded it ambigously so that people could cheat if they wanted to and send the numbers together as a batch - ie binary strings instead of single numbers. One team did cotton onto this approach and won the challenge.

This is my own implementation in C running on a single computer. The teams had four computers each at their disposal (the team member laptops). 


