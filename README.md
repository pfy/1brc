# Swift code for the 1billion row challange
This is a swift implementation for the 1 billion row challange on
https://github.com/gunnarmorling/1brc .

The idea is the following:
1. MMAP the file without loading it
2. Split the file at newline bounderies for every core
3. For every split, run a block on every core with an operation queue
4. Get one accumulator dictionary per thread
5. inside the block, get raw byte access and iterate over every byte
6. find the semicolon, while accumulating a byte contining the first 8 bytes of the name
7. still finding the semicolon, after 8 bytes only calculate the hash
8. use a pointer to the city name, do not copy anything
9. after the semicolon, get the sign or the first number. the number is stored as int
10. accumulate numbers until we reach a newline, ignoring the point since the test datas always have one number after the point
11.  generate a key, containing our hash as Int (Hashable needs an int ..)
12. find or update the statistics element for the city in our special hash. use the find function to get an index
13. merge the results from all threads
14. print the output

We use a special hashmap, which
* can use a predefined hash value
* can get us the index of a key or the next free element and supports insert by index
