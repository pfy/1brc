//
//  main.swift
//  scanner
//
//  Created by David Gunzinger on 03.01.2024.
//

import Foundation
import Accelerate

let arguments = CommandLine.arguments
if arguments.count != 2 {
    print("usage \(arguments[0]) measurements.txt")
    exit(-1)
}

let path = arguments[1]

let newline = "\n".data(using: .utf8)![0]
let semicolon = ";".data(using: .utf8)![0]
let zero = "0".data(using: .utf8)![0]
let zero11 = 11 * Int(zero)
let zero111 = 111 * Int(zero)
let point = ".".data(using: .utf8)![0]
let minus = "-".data(using: .utf8)![0]
let hash_offset_basis =   5381 as UInt64

var byCity = [DictionaryKey: Statistic]()
var byCityLock = NSRecursiveLock()


// 1: MMAP the file without loading it
let data = try! Data(contentsOf: URL(fileURLWithPath: path), options: .alwaysMapped)
let numberOfCores = ProcessInfo.processInfo.activeProcessorCount

var datas = [] as [(Int,Int)]

// 2: Split the file at newline bounderies for every core
var start = 0
let splits = numberOfCores
for i in 0..<splits {
    var end  =  data.count / splits * (i + 1) - 1
    while (data[end] != newline) {
        end += 1
    }
    datas.append((start, end))
    start = end + 1
}

let operationQueue = OperationQueue()
operationQueue.qualityOfService = .userInteractive

// 3: run a block on every core with an operation queue
func block(subdata: (Int,Int)) {
    // 4. Get one accumulator dictionary per thread
    let byCityThreaded = SimpleHashMap(capacity: 1024 * 50)
    
    // 5: inside the block, get raw byte access and iterate over every byte
    data.withUnsafeBytes { (bytes: UnsafeRawBufferPointer) in
        
        var pos = subdata.0
        let end = subdata.1
        var byte = 0 as UInt8
        
        while pos < end {
            byte = bytes[pos]

            // 6. find the semicolon, while accumulating a byte contining the first 8 bytes of the name
            var cityNameHashCode = hash_offset_basis
            var cityName8Bytes = 0 as UInt64
            let cityNameStart = pos
            var i = 0
            while byte != semicolon && i < 8 {
                if byte == semicolon {break}
                    cityNameHashCode = (cityNameHashCode << 5 &+ cityNameHashCode) &+ UInt64(byte)
                    cityName8Bytes = cityName8Bytes << 8 | UInt64(byte)
                    pos = pos &+ 1
                    i = i &+ 1
                    byte = bytes[pos]
            }
            // 7. still finding the semicolon, after 8 bytes only calculate the hash
            while byte != semicolon  {
                cityNameHashCode = (cityNameHashCode << 5 &+ cityNameHashCode) &+ UInt64(byte)
                pos = pos &+ 1
                byte = bytes[pos]
            }
            // 8. make a pointer to the hash
            let cityNameBytes = UnsafeRawBufferPointer(start: bytes.baseAddress!.advanced(by: cityNameStart), count: pos - cityNameStart)
            
            pos = pos &+ 1
            byte = bytes[pos]
            
            // debug: to read the current string
        //    var cityNameString = String(bytes: cityNameBytes, encoding: .utf8)
            var cityValue = 0 as Int
            var valueSign = 1
            
            // 9: get the sign or the first number. the number is stored as int
            if byte == minus {
                valueSign = -1;
                pos = pos &+ 1
            }
            
            // fancy number parsing, from https://github.com/dannyvankooten/1brc/blob/main/analyze.c#L39
            
            // 1.2\n
            if bytes[pos+1] == point {
                cityValue =  Int(bytes[pos]) * 10 + Int(bytes[pos + 2]) - zero11
                pos = pos &+ 4
            } else {
                // 22.3\n
               cityValue = Int(bytes[pos]) * 100 + Int(bytes[pos+1]) * 10 + Int(bytes[pos + 3]) - zero111
                pos = pos &+ 5
            }
            
            let value = cityValue * valueSign
            // 11: generate a key, containing our hash as Int (Hashable needs an int ..)
            let cityNameHashCodeInt = withUnsafeBytes(of: cityNameHashCode) {$0.load(as: Int.self)}
            let cityName = DictionaryKey(hashValue: cityNameHashCodeInt, cityName8Bytes: cityName8Bytes, bytes: cityNameBytes)
            
            // 12: find or update the statistics element for the city in our special hash. use the find function to get an index
            let hashIndex = byCityThreaded.find(key: cityName)
            if let statistic = byCityThreaded.valueAtIndex(index: hashIndex) {
                statistic.max = max(statistic.max, value);
                statistic.min = min(statistic.min, value);
                statistic.count += 1
                statistic.sum += value
            } else {
                byCityThreaded.insertAtIndex(index: hashIndex, key: cityName, value: Statistic(min: value, max: value, count: 1, sum: value, name: cityNameBytes))
            }
        }
        // 13. merge the results from all threads
        byCityLock.withLock {
            byCity = byCity.merging(byCityThreaded, uniquingKeysWith: { statistic, statistic2 in
                statistic.max = max(statistic.max, statistic2.max)
                statistic.min = min(statistic.min, statistic2.min)
                statistic.count = statistic.count + statistic2.count
                statistic.sum = statistic.sum + statistic2.sum
                return statistic
            })
        }
    }
}
for subdata in datas {
    operationQueue.addOperation {
        block(subdata: subdata)
    }
}

operationQueue.waitUntilAllOperationsAreFinished()

// 14 print the output
print("All tasks completed")
let output = byCity.values.map({ value in
    return (String(bytes: value.name, encoding: .utf8)!, value)
}).sorted(by: { a, b in
    return a.0 < b.0
}).map{ data in
    let statistics = data.1
    return  String(format: "%@=%.1f/%.1f/%.1f", data.0, Float(statistics.min) / 10 , Float(statistics.sum) / Float(statistics.count) / 10, Float(statistics.max) / 10)
}.joined(separator: ", ")
print("{\(output)}")


/*
 Simple hash map:
 * realloc is NOT implemented
 * there is a function to get the index of a key or the next free index. insert by index is supported
 * the hashmap can use a predefined hash
 */

class SimpleHashMap:  Collection, Sequence {
    private var _values: [Statistic?]
    private var _keys: [DictionaryKey?]
    private var _capacity: Int
    private var _capcityMask: Int
    func index(after i: Int) -> Int {
        var n = i+1
        while n < _keys.count &&  _keys[n] == nil && n < _capacity {
            n += 1
        }
        return n
    }
    var startIndex: Int { return index(after: -1) }
    var endIndex: Int   { return _capacity }
    subscript(index: Int) -> Element {
        return (_keys[index]!,_values[index]!)
    }
    
    typealias Element = (DictionaryKey, Statistic)
    
    
    init(capacity: Int) {
        let numberOfBits = Int(floor(log2(Double(capacity + 1))))
        _capcityMask = (1 << numberOfBits) - 1
        _capacity = capacity
        _values = Array(repeating: nil, count: capacity)
        _keys = Array(repeating: nil, count: capacity)
    }
    @inlinable subscript(key: DictionaryKey) -> Statistic? {
        get {
            let index = find(key: key)
            return _values[index]
            
        }
        set {
            let index = find(key: key)
            _values[index] = newValue
            _keys[index] = key
        }
    }
    
    @inlinable func find(key: DictionaryKey) -> Int {
        let hash = key.hashValue & _capcityMask
        var index = hash
        while _keys[index] != nil && _keys[index] != key {
            index = (index &+ 1) & _capcityMask
        }
        return index
    }
    @inlinable func valueAtIndex(index: Int) -> Statistic? {
        return _values[index]
    }
    @inlinable func insertAtIndex(index: Int, key: DictionaryKey, value: Statistic) {
        _values[index] = value
        _keys[index] = key
    }
    
    
    
}


class Statistic {
    var min: Int;
    var max: Int;
    var count: Int;
    var sum: Int;
    let name: UnsafeRawBufferPointer
    init(min: Int, max: Int, count: Int, sum: Int, name: UnsafeRawBufferPointer) {
        self.min = min
        self.max = max
        self.count = count
        self.sum = sum
        self.name = name
    }
}

struct DictionaryKey: Hashable {
    let hashValue: Int
    let cityName8Bytes: UInt64
    let bytes: UnsafeRawBufferPointer
    func hash(into hasher: inout Hasher) {
        withUnsafeBytes(of: hashValue) { rawBytes in
            hasher.combine(bytes: rawBytes)
        }
    }
    static func == (lhs: Self, rhs: Self) -> Bool {
        if rhs.hashValue != lhs.hashValue || rhs.cityName8Bytes != lhs.cityName8Bytes  {
            return false
        }
        let rhsCount = rhs.bytes.count
        let lhsCount = lhs.bytes.count
        if (rhsCount != lhsCount) {
            return false
        }
        // if the byte count is less than 8 and the first 8 bytes match we have an match
        if rhsCount < 8 {
            return true
        }
        for i in 8..<(rhsCount) {
            if lhs.bytes[i] != rhs.bytes[i] {
                return false
            }
        }
        return true
    }
}
