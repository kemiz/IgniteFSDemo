# Ignite Query Example

This is a simple Ignite example that demonstrates loading up a cache with market data and using SQL queries to filter results and run aggregations.

## Running the example 
1. Start the Server node and wait for it to load
  - Use "-load" as program argument to trigger the loading of data
  - To add more servers, use the same class but without `-load` as an argument
  - You can always adjust the number of entries by changing the `CACHE_SIZE` field

2. Run the Client and observe the results
  - Inspect the client code to see the use of queries
  - Configure the `PAGE_SIZE` field to print more results on screen as part of the test
