# flySTAT

<ins>Definition</ins>
- STAT *(adverb)*: "without delay : immediately" \[[Merriam-Webster](https://www.merriam-webster.com/dictionary/stat)\]

## Description


Have you ever wondered what future delays would look like for an airport, so you can plan accordingly and minimize traveling pains?
FlySTAT aims to unveil the unknown and provide accurate guidance for what future delays might look like. It will answer questions like:
- Which airports will have the least probability of delays in the future?
- Given an airport or airline, when is the best time to fly during a given window?
- What kind of delays can be expected for a given airport?

Using a range of evaluation techniques varying in complexity, from simple averaging, to weighted ranking based on delay propagation and linear regression, FlySTAT will provide results that bring useful insights to both airline industries and their customer bases.


## Obstacles and Methodology

TODO: edit this
To calculate the initial vector **v<sub>0</sub>**, which contains the average delay for a given day across past years, 2 different methods of calculating the averages came up:

1. Add up the total delay time across the date of every year in the past, along with the total number of flights across all those days. Then, take the global average.

2. Achieve the local average of every day across the past years by summing up the flight count and total delay time for a given day, then calculating the average delay for that day. The global average is achieved by taking the average of all the local averages.

We decided to go with the latter approach, as taking the average of the local averages more closely represents the average at that given day on any given year.
