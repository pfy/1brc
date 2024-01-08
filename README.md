# Swift code for the 1billion row challenge
This is a swift implementation for the 1 billion row challenge on
https://github.com/gunnarmorling/1brc .

The code is in https://github.com/pfy/1brc/blob/main/scanner/main.swift

The idea is the following:
1. MMAP the file without loading it
2. Split the file at newline boundaries for every core
3. For every split, run a block on every core with an operation queue
4. Get one accumulator dictionary per thread
5. inside the block, get raw byte access and iterate over every byte
6. find the semicolon, while accumulating a byte containing the first 8 bytes of the name
7. still finding the semicolon, after 8 bytes only calculate the hash
8. use a pointer to the city name, do not copy anything
9. after the semicolon, get the sign or the first number. the number is stored as int * 10. use the number parsing logic from https://github.com/dannyvankooten/1brc/blob/main/analyze.c#L39
10. accumulate numbers until we reach a newline, ignoring the point since the test datas always have one number after the point
11.  generate a key, containing our hash as Int (Hashable needs an int ..)
12. find or update the statistics element for the city in our special hash. use the find function to get an index
13. merge the results from all threads
14. print the output

We use a special hashmap, which
* can use a predefined hash value
* can get us the index of a key or the next free element and supports insert by index


## Results
(after warm, best of 3)

### on my m1 pro laptop

```
      Model Name: MacBook Pro
      Model Identifier: MacBookPro18,2
      Model Number: Z14Y0007KSM/A
      Chip: Apple M1 Max
      Total Number of Cores: 10 (8 performance and 2 efficiency)
      Memory: 64 GB
```
```
./scanner measurements.txt  17.53s user 1.36s system 796% cpu 2.371 total
```

### on a mac studio

```
      Model Name: Mac Studio
      Model Identifier: Mac13,2
      Model Number: Z14K0002CSM/A
      Chip: Apple M1 Ultra
      Total Number of Cores: 20 (16 performance and 4 efficiency)
      Memory: 64 GB
```
```
./scanner measurements.txt  19.40s user 2.55s system 1270% cpu 1.729 total
```
